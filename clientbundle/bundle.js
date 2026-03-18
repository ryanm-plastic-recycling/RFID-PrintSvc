import { PublicClientApplication } from "@azure/msal-browser";

window.createMsalInstance = function(config) {
  return new PublicClientApplication(config);
};
