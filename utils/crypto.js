import crypto from "crypto";
import dotenv from "dotenv";

dotenv.config();

const algorithm = "aes-256-cbc";
const encryptionKey = process.env.ENCRYPTION_KEY;
if (!encryptionKey) {
  throw new Error("ENCRYPTION_KEY is not defined in .env. Please set a valid base64-encoded key.");
}
const secretKey = Buffer.from(encryptionKey, "base64");

export const encryptPassword = (password) => {
  const iv = crypto.randomBytes(16);
  const cipher = crypto.createCipheriv(algorithm, secretKey, iv);
  const encrypted = Buffer.concat([cipher.update(password), cipher.final()]);
  return {
    iv: iv.toString("hex"),
    encryptedData: encrypted.toString("hex"),
  };
};

export const decryptPassword = (encryptedPassword, ivHex) => {
  try {
    const iv = Buffer.from(ivHex, "hex");
    const encryptedText = Buffer.from(encryptedPassword, "hex");
    const decipher = crypto.createDecipheriv(algorithm, secretKey, iv);
    const decrypted = Buffer.concat([
      decipher.update(encryptedText),
      decipher.final(),
    ]);
    return decrypted.toString();
  } catch (error) {
    console.error("Decryption error:", error);
    throw error;
  }
};