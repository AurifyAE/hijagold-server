import jwt from 'jsonwebtoken';
import dotenv from 'dotenv'
dotenv.config()
const SECRET_KEY = process.env.SECRET_KEY 

export const generateToken = (payload, expiresIn = '1h') => {
  return jwt.sign(payload, SECRET_KEY, { expiresIn });
};

// Function to verify a token and handle errors
export const verifyToken = (token) => {
  try {
    return jwt.verify(token, SECRET_KEY);
  } catch (error) {
    if (error.name === 'TokenExpiredError') {
      throw new Error('Token has expired.');
    } else if (error.name === 'JsonWebTokenError') {
      throw new Error('Invalid token.');
    }
    throw new Error('Token verification failed.');
  }
};
