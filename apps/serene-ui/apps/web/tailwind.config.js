import sharedConfig from "serene-client-shared/tailwind.config";

export default {
    presets: [sharedConfig],
    content: [
        "./src/**/*.{ts,tsx}",
        "./node_modules/my-shadcn-ui/src/**/*.{js,ts,jsx,tsx}",
    ],
};
