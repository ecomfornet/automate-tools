# This machine

An instabox sandbox: a Linux VM (Firecracker) in the cloud, not your laptop.
Written by instabox at 2026-11-04T11:13:30Z; rewritten for every new machine and at every start from stopped.

| | |
| --- | --- |
| Machine ID | `m_agq2mq4y2v6bk2vqseskvik6cu` |
| Created | 2026-11-04T11:13:30Z |
| Name | dev-box-1 |
| Template | agent-browser@2026-11-01.3-android.1-cli.1 |
| Size | 4 vCPU, 8192 MiB memory, 16 GiB root disk, 20 GiB /data |

## Facts

- User `user`, passwordless `sudo`. Home is /data/home; persistent work directory: /data/workspace.
- Both disks, the root disk and /data, are this machine's own and survive a stop.
  A stop ends every process; a suspend keeps memory and processes; a snapshot saves the root disk, and /data unless it is root-only.
- Outbound internet works. No port is reachable from the internet: people reach the
  machine through instabox (SSH, exec, the dashboard's Terminal and Computer tabs).

## How to use it

People, from their own computer:

- Dashboard: this machine's Terminal tab is a shell; its Computer tab shows the desktop.
- CLI: `instabox ssh m_agq2mq4y2v6bk2vqseskvik6cu` for a shell (or VS Code / Cursor through `instabox ssh-config m_agq2mq4y2v6bk2vqseskvik6cu`),
  `instabox exec m_agq2mq4y2v6bk2vqseskvik6cu -- <cmd>` to run one command, `instabox files m_agq2mq4y2v6bk2vqseskvik6cu [PATH]` to browse files.

Agents running on this machine:

This is reference, not a to-do list. Nothing below has to be checked, run or asked
for unless a task needs it.

- An agent on this machine is already inside it: its shell commands execute here
  directly, with no `instabox ssh` or `exec` in between. Persistent work directory: /data/workspace.
- Sign-ins: this machine may already be signed in to git, gh, clouds or agents (from
  before a stop, or copied from a snapshot's source). When a task needs one, it may
  already work (`gh auth status`, `git config --get user.email`).
- Public ports, when configured: `instabox-port publish 3000` gives a running HTTP service
  a public URL that stays until unpublished; `instabox-port serve ./report` serves a directory.
  With `--private` only the printed link (its `_token`) opens the URL; the link is printed
  once (`--private --rotate-token` prints a new one). URLs answer only while this sandbox runs.
  `instabox-port list` and `instabox-port unpublish <port>` manage them, as does its Ports
  tab in the dashboard.
- A browser people can watch: while the desktop runs (the Computer tab starts it), browsers that
  Playwright or Puppeteer launch from a new shell show on it, headed on `DISPLAY=:1`, even when
  asked for headless (`INSTABOX_HEADLESS=1` keeps them headless); `DISPLAY=:1 chromium &` opens one there too.
  The computer-use MCP (`instabox-computer-use`) drives the same desktop.
- The `instabox` MCP (Claude Code, Codex, OpenCode) answers how to work on this machine and, with a
  scoped API key in `INSTABOX_API_KEY`, lists, creates, starts, stops and runs commands on
  the person's other sandboxes.

## Found on this machine

- `git` (/usr/bin/git)
- `gh` (/usr/bin/gh)
- `curl` (/usr/bin/curl)
- `jq` (/usr/bin/jq)
- `node` (/usr/local/bin/node)
- `npm` (/usr/local/bin/npm)
- `pnpm` (/usr/local/bin/pnpm)
- `yarn` (/usr/local/bin/yarn)
- `bun` (/usr/local/bin/bun)
- `python3` (/usr/bin/python3)
- `uv` (/usr/local/bin/uv)
- `go` (/usr/local/bin/go)
- `cargo` (/usr/local/bin/cargo)
- `java` (/usr/bin/java)
- `javac` (/usr/bin/javac)
- `zip` (/usr/bin/zip)
- `docker` (/usr/bin/docker)
- `claude` (/usr/local/bin/claude)
- `codex` (/usr/local/bin/codex)
- `opencode` (/usr/local/bin/opencode)
- `chromium` (/usr/bin/chromium)
- `docker compose`

Desktop: installed. The Computer tab of the dashboard starts it (display :1, Xvnc on 127.0.0.1:5901).
