import { Dispatch, SetStateAction } from "react";

export interface AuthorizeGithubContextType {
    authorized: boolean;
    code: string | null;
    codeTimeLeft: number | null;
    authLink: string | null;
    startAuthorization: () => void;
    verifyAuthorization: () => void;
    unauthorizeGithub: () => void;
    starAllowed: boolean;
    setStarAllowed: Dispatch<SetStateAction<boolean>>;
}
