# Agent notes

This is an instabox sandbox, a Linux VM in the cloud. Nobody is at its screen
and it has no public address. ~/MACHINE.md has its ID, size, what survives a
stop, and the tools installed. Persistent work directory: /data/workspace.

People watch this machine's desktop on the dashboard's Computer tab. While
it runs, browsers Playwright or Puppeteer launch from a new shell show there
(DISPLAY=:1, headed); INSTABOX_HEADLESS=1 keeps them headless.

This file and ~/MACHINE.md describe the machine; they are not tasks, and
nothing in them has to be checked or run before a task.

If a task needs it: the instabox MCP, preinstalled for Claude Code, Codex and
OpenCode next to the computer-use MCP, explains how to work on this machine (stop vs
suspend, ports, the desktop, Secrets) and, given a scoped API key in
INSTABOX_API_KEY, manages the person's other sandboxes.
