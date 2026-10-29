import React, { createContext, useContext } from "react";
import { useTranslation } from "react-i18next";
import type { TFunction, i18n as I18n } from "i18next";

type Languages = {
    en: { nativeName: string };
};

interface LanguageContextValue {
    t: TFunction<"translation", undefined>;
    i18n: I18n;
    changeLanguage: (language: string) => void;
    languages: Languages;
}

export const LanguageContext = createContext<LanguageContextValue | undefined>(
    undefined,
);

interface WithLanguageProps {
    children: React.ReactNode;
}

export const WithLanguage = ({ children }: WithLanguageProps) => {
    const languages: Languages = {
        en: { nativeName: "English" },
    };

    const { t, i18n } = useTranslation();

    const changeLanguage = (language: string) => {
        i18n.changeLanguage(language);
    };

    return (
        <LanguageContext.Provider
            value={{ t, i18n, changeLanguage, languages }}>
            {children}
        </LanguageContext.Provider>
    );
};

export const useLanguageContext = (): LanguageContextValue => {
    const ctx = useContext(LanguageContext);
    if (!ctx) {
        throw new Error(
            "useLanguageContext must be used within a WithLanguage provider",
        );
    }
    return ctx;
};
