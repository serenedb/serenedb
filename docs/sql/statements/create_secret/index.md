---
title: CREATE SECRET
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';
import DocCallout from "@site/src/components/DocCallout";

The `CREATE SECRET` statement creates a new secret in the [Secrets Manager](../../../configuration/secrets_manager.md).

## Syntax for `CREATE SECRET`

<RailroadDiagram source={RailroadSource} production="rrdiagram1" />

<DocCallout type="attention">
Warning When using the command line client, the `CREATE SECRET` statements are stored in your SereneDB history as plain text.
</DocCallout>

## Syntax for `DROP SECRET`

<RailroadDiagram source={RailroadSource} production="rrdiagram2" />
