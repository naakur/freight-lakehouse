# Databricks CLI on Windows — Installation and Authentication

**Purpose:** Reference guide for installing the Databricks CLI on Windows and authenticating to a Databricks Free Edition workspace.

> **Important:** Databricks Free Edition is not Azure Databricks. Use the URL of your actual Free Edition workspace. Do not use your Azure Portal URL or assume Azure service-principal authentication is required for interactive CLI use.

## 1. Prerequisites

- Windows with Command Prompt or PowerShell.
- Access to your Databricks Free Edition workspace.
- A browser in which you can sign in to Databricks.

## 2. Find the CLI package

Open **Command Prompt** and run:

```cmd
winget search databricks
```

The official CLI package should appear as:

- **Package ID:** `Databricks.DatabricksCLI`

`winget search` only searches the package catalogue; it does not install the CLI.

## 3. Install the Databricks CLI

Run:

```cmd
winget install --id Databricks.DatabricksCLI --exact
```

When the installation finishes:

1. Close all Command Prompt and PowerShell windows.
2. Open a new terminal.
3. Verify the installation:

```cmd
databricks -v
```

You can also check whether Windows can find the executable:

```cmd
where databricks
```

## 4. Troubleshooting: `databricks` is not recognized

If you see:

```text
'databricks' is not recognized as an internal or external command,
operable program or batch file.
```

Try the following steps in order.

### A. Check whether Winget lists the package as installed

```cmd
winget list --id Databricks.DatabricksCLI
```

### B. Check whether the command is on PATH

```cmd
where databricks
```

If the package is installed but `where` finds nothing, restart the terminal first. If that does not help, check the installation details and PATH before reinstalling.

### C. Retry installation if it is not installed

```cmd
winget install --id Databricks.DatabricksCLI --exact
```

If Winget reports that the package is already installed, consult Winget's available repair/upgrade options or the official Databricks installation guide rather than guessing the executable path.

**Do not assume the executable is at `C:\Windows\databricks.exe` for every installation method.** Use the path reported by your installation or `where databricks`.

## 5. Copy your Free Edition workspace URL

1. Sign in to [Databricks](https://www.databricks.com/).
2. Open your **Free Edition workspace**.
3. Copy the complete workspace URL from the browser address bar.

Use the workspace URL, not the general Databricks homepage or Azure Portal URL.

## 6. Authenticate with browser-based OAuth

In Command Prompt, replace the example URL with your actual workspace URL:

```cmd
databricks auth login --host "https://YOUR-WORKSPACE-URL" --profile FREE_EDITION
```

For example, the host will resemble your actual workspace's URL; do not copy a sample URL from this guide.

Expected flow:

1. The CLI starts browser-based sign-in/authorization.
2. Sign in with the account that has access to the Free Edition workspace.
3. Complete the authorization prompts.
4. Return to the terminal.

The profile name `FREE_EDITION` is a local label chosen for convenience. You can choose another name.

## 7. Verify authentication

List configured authentication profiles:

```cmd
databricks auth profiles
```

Check the authenticated user:

```cmd
databricks current-user me --profile FREE_EDITION
```

If the command returns details for your Databricks user, authentication is working.

As a further basic workspace check, try:

```cmd
databricks workspace list / --profile FREE_EDITION
```

If this command fails, read the error carefully. Authentication can succeed while a particular API operation or workspace path is unsupported or unavailable.

## 8. Use the profile in later commands

Include the profile option in CLI commands:

```cmd
databricks <command> --profile FREE_EDITION
```

Replace `<command>` with the Databricks CLI command you want to run.

## 9. Security and identity notes

- Do not paste passwords, access tokens, OAuth tokens, or secrets into notes, source control, or chat.
- Do not assume CLI authentication automatically grants access to data or cloud resources.
- Access to Azure Key Vault, ADLS, and other Azure resources requires the appropriate identity setup and permissions separately.
- For local interactive development, browser-based OAuth is generally the simplest starting point.
- For automated deployments, use an appropriate non-interactive identity and follow your organisation's security guidance. Confirm the identity type and supported authentication method for the target workspace before configuring automation.

## 10. Quick command checklist

Run these commands in order:

```cmd
winget search databricks
winget install --id Databricks.DatabricksCLI --exact
```

Close and reopen the terminal, then:

```cmd
databricks -v
where databricks
```

After copying the Free Edition workspace URL:

```cmd
databricks auth login --host "https://YOUR-WORKSPACE-URL" --profile FREE_EDITION
databricks auth profiles
databricks current-user me --profile FREE_EDITION
databricks workspace list / --profile FREE_EDITION
```

## Official documentation

- [Databricks CLI installation](https://docs.databricks.com/aws/en/dev-tools/cli/install)
- [Databricks CLI authentication](https://docs.databricks.com/aws/en/dev-tools/cli/authentication)
- [Databricks Free Edition](https://www.databricks.com/product/free-edition)

> **Troubleshooting tip:** If a command fails, keep the exact command and error message (redact workspace identifiers or other sensitive information if needed). The exact error is more useful than reinstalling repeatedly.
