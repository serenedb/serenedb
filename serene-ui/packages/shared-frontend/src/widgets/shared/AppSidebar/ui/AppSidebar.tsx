import { NavBottom } from "./parts/NavBottom";
import { NavMain } from "./parts/NavMain";
import { NavTop } from "./parts/NavTop";

export const AppSidebar = ({}) => {
    return (
        <div className="flex flex-col h-dvh w-12 border-r-[0.5px]">
            <NavTop />
            <NavMain />
            <NavBottom />
        </div>
    );
};
