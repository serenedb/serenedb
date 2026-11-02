import { createContext, useContext } from "react";
import { AuthorizeGithubContextType } from "./types";

export const AuthorizeGithubContext = createContext<
    AuthorizeGithubContextType | undefined
>(undefined);

export const useAuthorizeGithub = (): AuthorizeGithubContextType => {
    const context = useContext(AuthorizeGithubContext);
    if (!context) {
        throw new Error(
            "useAuthorizeGithub must be used within a AuthorizeGithubProvider",
        );
    }
    return context;
};
