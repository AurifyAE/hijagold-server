
// Helper to create custom errors with status codes
export const createAppError = (message, statusCode) => {
    const error = new Error(message);
    error.statusCode = statusCode;
    error.isOperational = true; // Indicates it's a known error
    return error;
  };
  
  // Global error handler middleware
  export const errorHandler = (err, req, res, next) => {
    const statusCode = err.statusCode || 500;
    const message = err.message || "Internal Server Error";
  
    console.error("Error Stack:", err.stack);
  
    if (!err.isOperational) {
      console.error("Unexpected Error:", err);
      // If the error isn't operational, log and send a generic message
      res.status(500).json({ success: false, message: "Something went wrong!" });
    } else {
      // If it's an operational error, send the message and status code
      res.status(statusCode).json({
        success: false,
        message,
        error: process.env.NODE_ENV === "development" ? err.stack : message,
      });
    }
  };
  