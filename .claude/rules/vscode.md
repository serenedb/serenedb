---
paths:
  - ".vscode/**"
---

# VSCode Setup

## Profile

We have a VSCode profile which has already all the extensions which are needed (for instance for code navigation). Here is how to set it up:

0. Open a folder with SereneDB.
1. Create a `serenedb-cpp.code-profile` file in the root and paste the profile config below.
2. Open a VSCode command palette via default combination: Ctrl+Shift+P / Cmd+Shift+P for macOS.
3. Write in the palette `Open Profiles` and choose `Preferences: Open Profiles (UI)`.
4. In the UI of the profiles click on the down arrow which is located left to the `New Profile` button.
5. Choose import profile and specify a path to the `serenedb-cpp.code-profile`.
6. Create the profile and switch to it.
7. If a message appears offering to download the clangd server, accept it.

Now you can use C++ code navigation by Ctrl+Click (Cmd+Click for macOS)!

<details>
<summary>Profile config</summary>

```json
{
  "name": "SereneDB C++ template",
  "settings": "{\"settings\":\"{\\n    \\\"window.titleBarStyle\\\": \\\"custom\\\",\\n    \\\"files.trimFinalNewlines\\\": true,\\n    \\\"files.insertFinalNewline\\\": true,\\n    \\\"workbench.settings.applyToAllProfiles\\\": [\\n        \\\"files.insertFinalNewline\\\",\\n        \\\"files.trimFinalNewlines\\\",\\n        \\\"editor.inlayHints.enabled\\\",\\n        \\\"remote.autoForwardPorts\\\",\\n        \\\"files.autoSave\\\",\\n        \\\"editor.minimap.enabled\\\"\\n    ],\\n    \\\"editor.inlayHints.enabled\\\": \\\"off\\\",\\n    \\\"remote.autoForwardPorts\\\": false,\\n    \\\"files.autoSave\\\": \\\"afterDelay\\\",\\n    \\\"settingsSync.ignoredSettings\\\": [\\n        \\\"-clangd.path\\\"\\n    ],\\n    \\\"clangd.arguments\\\": [\\n        \\\"--compile-commands-dir=${workspaceFolder}/build\\\",\\n        \\\"--function-arg-placeholders=0\\\",\\n        \\\"--header-insertion=never\\\"\\n    ],\\n    \\\"window.newWindowProfile\\\": \\\"Default\\\",\\n    \\\"editor.minimap.enabled\\\": false,\\n    \\\"compilerexplorer.compilationDirectory\\\": \\\"${workspaceFolder}/build_rel\\\",\\n    \\\"editor.defaultFormatter\\\": \\\"llvm-vs-code-extensions.vscode-clangd\\\",\\n    \\\"extensions.ignoreRecommendations\\\": true,\\n    \\\"clangd.checkUpdates\\\": true,\\n    \\\"editor.tabSize\\\": 2,\\n    \\\"workbench.remoteIndicator.showExtensionRecommendations\\\": false\\n}\\n\"}",
  "extensions": "[{\"identifier\":{\"id\":\"github.remotehub\",\"uuid\":\"fc7d7e85-2e58-4c1c-97a3-2172ed9a77cd\"},\"displayName\":\"GitHub Repositories\",\"applicationScoped\":false},{\"identifier\":{\"id\":\"harikrishnan94.cxx-compiler-explorer\",\"uuid\":\"68ef4789-1f8c-4d80-b929-cfb718979aa2\"},\"displayName\":\"C/C++ Compiler explorer\",\"applicationScoped\":false},{\"identifier\":{\"id\":\"llvm-vs-code-extensions.vscode-clangd\",\"uuid\":\"103154cb-b81d-4e1b-8281-c5f4fa563d37\"},\"displayName\":\"clangd\",\"applicationScoped\":false},{\"identifier\":{\"id\":\"ms-vscode-remote.remote-containers\",\"uuid\":\"93ce222b-5f6f-49b7-9ab1-a0463c6238df\"},\"displayName\":\"Dev Containers\",\"applicationScoped\":false},{\"identifier\":{\"id\":\"ms-vscode-remote.remote-ssh\",\"uuid\":\"607fd052-be03-4363-b657-2bd62b83d28a\"},\"displayName\":\"Remote - SSH\",\"applicationScoped\":false},{\"identifier\":{\"id\":\"ms-vscode-remote.remote-ssh-edit\",\"uuid\":\"bfeaf631-bcff-4908-93ed-fda4ef9a0c5c\"},\"displayName\":\"Remote - SSH: Editing Configuration Files\",\"applicationScoped\":false},{\"identifier\":{\"id\":\"ms-vscode-remote.vscode-remote-extensionpack\",\"uuid\":\"23d72dfc-8dd1-4e30-926e-8783b4378f13\"},\"displayName\":\"Remote Development\",\"applicationScoped\":false},{\"identifier\":{\"id\":\"ms-vscode.remote-explorer\",\"uuid\":\"11858313-52cc-4e57-b3e4-d7b65281e34b\"},\"displayName\":\"Remote Explorer\",\"applicationScoped\":false},{\"identifier\":{\"id\":\"ms-vscode.remote-repositories\",\"uuid\":\"cf5142f0-3701-4992-980c-9895a750addf\"},\"displayName\":\"Remote Repositories\",\"applicationScoped\":false},{\"identifier\":{\"id\":\"ms-vscode.remote-server\",\"uuid\":\"105c0b3c-07a9-4156-a4fc-4141040eb07e\"},\"displayName\":\"Remote - Tunnels\",\"applicationScoped\":false},{\"identifier\":{\"id\":\"vadimcn.vscode-lldb\",\"uuid\":\"bee31e34-a44b-4a76-9ec2-e9fd1439a0f6\"},\"displayName\":\"CodeLLDB\",\"applicationScoped\":false}]"
}
```

</details>

<p align="center">
  <img src="https://github.com/user-attachments/assets/02f2e2f9-b9d6-407d-832a-2517254dee98" width="800" style="border-radius: 8px;">
</p>

## Debugging

VSCode provides a convenient way to debug code. Create a `.vscode/launch.json` file:

```json
{
  "configurations": [
    {
      "type": "lldb",
      "request": "attach",
      "name": "attach-to-serened",
      "program": "${workspaceFolder}/build/bin/serened"
    },
    {
      "type": "lldb",
      "request": "launch",
      "name": "iresearch",
      "program": "${workspaceFolder}/build/bin/iresearch-tests",
      "args": ["--gtest_filter=*PhraseFilterTestCase*"],
      "cwd": "${workspaceFolder}"
    }
  ]
}
```

Click **Run and Debug** on the left sidebar (Shift+Ctrl+D / Shift+Cmd+D). This adds two actions -- `attach-to-serened` for attaching to a running instance and `iresearch` to launch unit tests with the debugger. Use the dropdown next to the green triangle to pick one.

<p align="center">
  <img src="https://github.com/user-attachments/assets/fa246b5d-ebea-4598-8705-c252fbff5a0d" width="800" style="border-radius: 8px;">
</p>

## Sqllogic test highlighting

`.test` files are Plain Text until you install the VSCode extension that ships
with the runner: it colors SQL bodies as SQL, sqllogictest-rs directives as
keywords, and `#` comments as comments. It lives beside the parser whose syntax
it tracks, so the build-and-install steps -- including what to run when the
`code` CLI is not on `$PATH`, as over SSH -- are in
[`third_party/sqllogictest-rs/README.md`](../../third_party/sqllogictest-rs/README.md).
