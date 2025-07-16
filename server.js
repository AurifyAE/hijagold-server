import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';
import { Server } from 'socket.io';
import http from 'http';
import adminRouter from './routers/admin/index.js';
import superAdminRouter from './routers/superAdmin/index.js';
import chatRouter from './routers/chat/index.js';
import { mongodb } from './config/connection.js';
import { errorHandler } from './utils/errorHandler.js';
import mongoose from 'mongoose';
import Account from './models/AccountSchema.js'
dotenv.config();

const app = express();
const server = http.createServer(app);
const port = process.env.PORT || 4444;

// Initialize Socket.IO with CORS
const io = new Server(server, {
  cors: {
    origin: ['http://localhost:5173','https://crm-live-pro.onrender.com', 'http://localhost:3000', 'http://localhost:8080'],
    methods: ['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'x-secret-key', 'Authorization'],
    credentials: true,
  },
});

// Socket.IO connection handling
io.on('connection', (socket) => {
  console.log('Client connected:', socket.id);

  // Subscribe to balance updates for a specific user
  socket.on('subscribeBalance', async ({ userId }) => {
    if (!mongoose.isValidObjectId(userId)) {
      socket.emit('balanceError', { message: 'Invalid user ID' });
      return;
    }

    try {
      // Join user-specific room
      socket.join(userId);
      console.log(`Client ${socket.id} subscribed to balance updates for user: ${userId}`);

      // Fetch and emit initial balance
      const account = await Account.findById(userId).select('AMOUNTFC reservedAmount');
      if (!account) {
        socket.emit('balanceError', { message: 'Account not found' });
        return;
      }

      socket.emit('balanceUpdate', {
        userId,
        balance: account.AMOUNTFC || 0,
       reservedAmount: account.reservedAmount || 0,

      });
    } catch (error) {
      socket.emit('balanceError', { message: `Failed to subscribe: ${error.message}` });
      console.error(`Error subscribing user ${userId}:`, error.message);
    }
  });

  // Handle client disconnection
  socket.on('disconnect', () => {
    console.log('Client disconnected:', socket.id);
  });
});

// Make io accessible to routers
app.set('io', io);

app.use(express.static('public'));
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ limit: '50mb', extended: true }));

const corsOptions = {
  origin: ['http://localhost:5173','https://crm-live-pro.onrender.com', 'http://localhost:3000', 'http://localhost:8080'],
  methods: ['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'x-secret-key', 'Authorization'],
  credentials: true,
  optionsSuccessStatus: 204,
};

app.use(cors(corsOptions));

// Database connection
mongodb();

// Routes
app.use('/api/admin', adminRouter);
app.use('/api', superAdminRouter);
app.use('/api/chat', chatRouter);

// Global error handling
app.use(errorHandler);

server.listen(port, () => {
  console.log(`Server running at http://localhost:${port}`);
});