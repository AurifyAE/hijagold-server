import ExcelJS from 'exceljs';
import { getUserLedger } from './ledgerService.js';
import Order from '../../models/OrderSchema.js';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { dirname } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

/**
 * Generate Excel statement for a user
 * @param {String} userId - User account ID
 * @param {Object} options - Options for statement generation
 * @returns {Object} Result with file path or error
 */
export const generateExcelStatement = async (userId, options = {}) => {
  try {
    const {
      startDate = new Date(new Date().setDate(new Date().getDate() - 90)), // Default 90 days back
      endDate = new Date(),
      includeOpenPositions = true
    } = options;
    
    // Fetch user's ledger entries
    const ledgerResult = await getUserLedger(userId, {
      limit: 100, // Get more entries for the statement
      sortOrder: 'asc' // Oldest to newest for statement
    });
    
    if (!ledgerResult.success) {
      return { 
        success: false, 
        message: ledgerResult.message || "Failed to fetch ledger data"
      };
    }
    
    // Get user's open positions if requested
    let openPositions = [];
    if (includeOpenPositions) {
      openPositions = await Order.find({ 
        user: userId,
        status: "OPEN" 
      }).sort({ openingDate: 1 }).lean();
    }
    
    // Create a new Excel workbook
    const workbook = new ExcelJS.Workbook();
    workbook.creator = 'Hija Global Markets';
    workbook.lastModifiedBy = 'Trading System';
    workbook.created = new Date();
    workbook.modified = new Date();
    
    // Add a sheet for transactions
    const transactionSheet = workbook.addWorksheet('Statement', {
      properties: { tabColor: { argb: '4CAF50' } }
    });
    
    // Set up header row with styling
    transactionSheet.columns = [
      { header: 'SL.NO.', key: 'slNo', width: 8 },
      { header: 'ORDER NO.', key: 'orderNo', width: 15 },
      { header: 'OPEN DATE', key: 'openDate', width: 15 },
      { header: 'OPEN POSITION', key: 'openPosition', width: 15 },
      { header: 'QTY', key: 'qty', width: 10 },
      { header: 'OPEN PRICE', key: 'openPrice', width: 12 },
      { header: 'CLOSE DATE', key: 'closeDate', width: 15 },
      { header: 'CLOSE PRICE', key: 'closePrice', width: 12 },
      { header: 'P/L IN AED', key: 'pnl', width: 12 }
    ];
    
    // Style the header row
    transactionSheet.getRow(1).font = { bold: true };
    transactionSheet.getRow(1).fill = {
      type: 'pattern',
      pattern: 'solid',
      fgColor: { argb: 'E0E0E0' }
    };
    
    // Extract order data from ledger entries
    const ordersData = [];
    let totalPnL = 0;
    let totalQuantity = 0;
    
    // Process ledger entries to extract order information
    const processedOrderIds = new Set();
    
    ledgerResult.data.entries.forEach(entry => {
      if (entry.entryType === "ORDER" && entry.orderDetails && !processedOrderIds.has(entry.orderDetails.orderId)) {
        // Only process each order once
        processedOrderIds.add(entry.orderDetails.orderId);
        
        if (entry.orderDetails.status === "CLOSED") {
          const openDate = new Date(entry.orderDetails.openingDate).toLocaleDateString();
          const closeDate = new Date(entry.orderDetails.closingDate).toLocaleDateString();
          
          const profit = entry.orderDetails.profit || 0;
          totalPnL += profit;
          totalQuantity += entry.orderDetails.volume;
          
          ordersData.push({
            slNo: ordersData.length + 1,
            orderNo: '****' + entry.orderDetails.orderId.toString().slice(-3),
            openDate: openDate,
            openPosition: entry.orderDetails.type,
            qty: entry.orderDetails.volume + 'TTB',
            openPrice: entry.orderDetails.entryPrice?.toFixed(2) + '$',
            closeDate: closeDate,
            closePrice: entry.orderDetails.exitPrice?.toFixed(2) + '$',
            pnl: profit.toFixed(0)
          });
        }
      }
    });
    
    // Add rows to the worksheet
    transactionSheet.addRows(ordersData);
    
    // Add a total row
    transactionSheet.addRow({
      openPosition: 'TOTAL',
      qty: totalQuantity + 'TTB',
      pnl: totalPnL.toFixed(0)
    });
    
    // Style the total row
    const totalRow = transactionSheet.lastRow;
    totalRow.font = { bold: true };
    totalRow.getCell('openPosition').fill = {
      type: 'pattern',
      pattern: 'solid',
      fgColor: { argb: 'FFF9C4' }
    };
    totalRow.getCell('qty').fill = {
      type: 'pattern',
      pattern: 'solid',
      fgColor: { argb: 'FFF9C4' }
    };
    totalRow.getCell('pnl').fill = {
      type: 'pattern',
      pattern: 'solid',
      fgColor: { argb: 'FFF9C4' }
    };
    
    // Add a section for open positions
    if (openPositions.length > 0) {
      // Add a blank row
      transactionSheet.addRow({});
      
      // Add heading for open positions
      const openPositionsHeadingRow = transactionSheet.addRow({
        slNo: 'OPEN POSITION & PROFIT OR LOSS AT CURRENT PRICE'
      });
      openPositionsHeadingRow.font = { bold: true };
      openPositionsHeadingRow.getCell('slNo').fill = {
        type: 'pattern',
        pattern: 'solid',
        fgColor: { argb: 'E0E0E0' }
      };
      
      // Merge cells for the heading
      transactionSheet.mergeCells(`A${openPositionsHeadingRow.number}:I${openPositionsHeadingRow.number}`);
      
      // Add open positions header
      const openPosHeaderRow = transactionSheet.addRow({
        slNo: 'No. Of TTB',
        orderNo: 'Open Rate',
        openDate: 'Open Position',
        openPosition: 'Market Price',
        qty: 'Loss @ Market Rate'
      });
      openPosHeaderRow.font = { bold: true };
      openPosHeaderRow.eachCell((cell) => {
        cell.fill = {
          type: 'pattern',
          pattern: 'solid',
          fgColor: { argb: '90CAF9' }
        };
      });
      
      // Add open position data
      openPositions.forEach(position => {
        // Calculate current P&L based on latest market data (placeholder)
        // In real implementation, you should use actual market data
        const marketPrice = 1926.48; // Placeholder, use real-time data
        const pnl = position.type === 'BUY' 
          ? (marketPrice - position.openingPrice) * position.volume
          : (position.openingPrice - marketPrice) * position.volume;
        
        transactionSheet.addRow({
          slNo: position.volume,
          orderNo: position.openingPrice.toFixed(2),
          openDate: position.type.toLowerCase(),
          openPosition: marketPrice.toFixed(2),
          qty: pnl.toFixed(2)
        });
      });
    }
    
    // Add a title at the top
    transactionSheet.spliceRows(1, 0, [], [], [], [], [], []);
    transactionSheet.mergeCells('A1:I3');
    const titleCell = transactionSheet.getCell('A1');
    titleCell.value = 'HIJA GLOBAL MARKETS';
    titleCell.font = {
      name: 'Arial',
      family: 4,
      size: 22,
      bold: true,
      color: { argb: '4CAF50' }
    };
    titleCell.alignment = { horizontal: 'center', vertical: 'middle' };
    
    // Add user details
    const userHeader = transactionSheet.getCell('A4');
    // Get the first ledger entry to get the user ID/account number
    const accountId = ledgerResult.data.entries.length > 0 
      ? ledgerResult.data.entries[0].user.toString().slice(-7) 
      : userId.toString().slice(-7);
    
    // Format date range for statement title
    const startDateFormatted = startDate.toLocaleDateString();
    const endDateFormatted = endDate.toLocaleDateString();
    userHeader.value = `${accountId} STATEMENT FROM ${startDateFormatted} TO ${endDateFormatted}`;
    
    transactionSheet.mergeCells('A4:I4');
    userHeader.fill = {
      type: 'pattern',
      pattern: 'solid',
      fgColor: { argb: 'CCCCCC' }
    };
    userHeader.font = { bold: true };
    userHeader.alignment = { horizontal: 'center' };
    
    // Set borders for all cells with data
    transactionSheet.eachRow((row, rowNumber) => {
      row.eachCell((cell) => {
        cell.border = {
          top: {style:'thin'},
          left: {style:'thin'},
          bottom: {style:'thin'},
          right: {style:'thin'}
        };
      });
    });
    
    // Ensure directory exists
    const statementsDir = path.join(__dirname, '../../../statements');
    if (!fs.existsSync(statementsDir)) {
      fs.mkdirSync(statementsDir, { recursive: true });
    }
    
    // Generate unique filename
    const fileName = `statement_${userId}_${Date.now()}.xlsx`;
    const filePath = path.join(statementsDir, fileName);
    
    // Save the workbook
    await workbook.xlsx.writeFile(filePath);
    
    return {
      success: true,
      filePath,
      fileName
    };
  } catch (error) {
    console.error("Error generating Excel statement:", error);
    return {
      success: false,
      message: error.message || "Failed to generate Excel statement"
    };
  }
};

export default {
  generateExcelStatement
};