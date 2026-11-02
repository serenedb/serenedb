import { oc } from "@orpc/contract";
import {
    AddConnectionInput,
    DeleteConnectionInput,
    UpdateConnectionInput,
} from "../inputs";
import {
    AddConnectionOutput,
    DeleteConnectionOutput,
    ListMyConnectionOutput,
    UpdateConnectionOutput,
} from "../outputs";

export const listMyConnectionContract = oc.output(ListMyConnectionOutput);
export const addConnectionContract = oc
    .input(AddConnectionInput)
    .output(AddConnectionOutput);
export const updateConnectionContract = oc
    .input(UpdateConnectionInput)
    .output(UpdateConnectionOutput);
export const deleteConnectionContract = oc
    .input(DeleteConnectionInput)
    .output(DeleteConnectionOutput);

export const connectionContracts = {
    listMy: listMyConnectionContract,
    add: addConnectionContract,
    update: updateConnectionContract,
    delete: deleteConnectionContract,
};
