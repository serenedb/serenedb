import { oc } from "@orpc/contract";
import {
    AddSavedQueryInput,
    DeleteSavedQueryInput,
    UpdateSavedQueryInput,
} from "../inputs/saved-queries.inputs";
import {
    AddSavedQueryOutput,
    DeleteSavedQueryOutput,
    ListMySavedQueriesOutput,
    UpdateSavedQueryOutput,
} from "../outputs/saved-queries.outputs";

export const listMySavedQueries = oc.output(ListMySavedQueriesOutput);
export const addSavedQuery = oc
    .input(AddSavedQueryInput)
    .output(AddSavedQueryOutput);
export const updateSavedQuery = oc
    .input(UpdateSavedQueryInput)
    .output(UpdateSavedQueryOutput);
export const deleteSavedQuery = oc
    .input(DeleteSavedQueryInput)
    .output(DeleteSavedQueryOutput);

export const savedQueryContracts = {
    listMy: listMySavedQueries,
    add: addSavedQuery,
    update: updateSavedQuery,
    delete: deleteSavedQuery,
};
