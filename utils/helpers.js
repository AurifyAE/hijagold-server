export const cleanPhoneNumber = (phoneNumber) => {
  return phoneNumber.replace("whatsapp:", "").replace("+", "");
};