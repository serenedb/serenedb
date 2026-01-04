import i18next from "i18next";
import { initReactI18next } from "react-i18next";

import English from "../translations/English.json";

const resources = {
    en: {
        translation: English,
    },
};

i18next.use(initReactI18next).init({
    resources,
    lng: "en",
});

export default i18next;
