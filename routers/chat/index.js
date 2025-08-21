import express from "express";
import { handleWhatsAppWebhook } from "../../controllers/chat/whatsappController.js";


const router = express.Router();


router.post("/whatsapp-webhook", handleWhatsAppWebhook);


export default router;