import { SupportIcon } from "@serene-ui/shared-frontend/shared";
import { CommandSection } from "../../model";
import { CommandItemBase } from "./CommandItemBase";
import { useSupportModal } from "@serene-ui/shared-frontend/features";

export interface CreateIssueCommandProps {}

export const CreateIssueCommand = ({}: CreateIssueCommandProps) => {
    const { setOpen } = useSupportModal();

    return (
        <CommandItemBase
            isSection={false}
            title={"Create GitHub issue"}
            icon={<SupportIcon />}
            section={CommandSection.Utilities}
            sectionLabel="Utilities"
            onSelect={() => setOpen(true)}
        />
    );
};
