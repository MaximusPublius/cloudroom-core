//! `cloudroom computer-use`: desktop apps on a virtual Linux screen in a Cloud sandbox.
//! The sandbox image installs Xvfb, AT-SPI and a pinned Cua Driver (web/computer-use.sh).
//! Everything here runs as the agent and starts lazily on the first call. The sandbox is the
//! boundary, so apps need no per-app approval here (docs/scopes/computer-use.md).

use std::os::unix::net::UnixStream;
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

pub const SKILL: &str = include_str!("SKILL.md");

const DRIVER: &str = "/usr/local/lib/cloudroom/cua-driver/cua-driver";
const DISPLAY: &str = ":99";
const X_SOCKET: &str = "/tmp/.X11-unix/X99";
const STATE: &str = "/tmp/cloudroom-computer-use";
const SESSION: &str = "cloudroom";
// Upstream tools that accept a `session` label. One label keeps screenshots valid across calls.
const SESSION_TOOLS: &[&str] = &[
    "browser_click",
    "browser_dialog",
    "browser_download",
    "browser_navigate",
    "browser_pointer",
    "browser_prepare",
    "browser_set_input_files",
    "browser_type",
    "click",
    "clipboard_read",
    "clipboard_write",
    "double_click",
    "drag",
    "end_session",
    "get_agent_cursor_state",
    "get_browser_state",
    "get_cursor_position",
    "get_desktop_state",
    "get_screen_size",
    "get_session",
    "get_window_state",
    "hotkey",
    "invoke_menu",
    "move_cursor",
    "press_key",
    "right_click",
    "scroll",
    "set_agent_cursor_enabled",
    "set_agent_cursor_motion",
    "set_agent_cursor_theme",
    "set_value",
    "set_window_frame",
    "start_recording",
    "start_session",
    "type_text",
    "verify_state",
];
const BLOCKED_TOOLS: &[&str] = &["set_config", "install_extension", "check_for_update"];
const USAGE: &str = "Usage: cloudroom computer-use status | start | launch <command> [args...] | tools [<tool>] | call <tool> ['<json-args>']";

type Error = Box<dyn std::error::Error>;

fn bus() -> String {
    format!("unix:path={STATE}/bus")
}

fn desktop_env(command: &mut Command) -> &mut Command {
    command
        .env("DISPLAY", DISPLAY)
        .env("DBUS_SESSION_BUS_ADDRESS", bus())
        .env("CUA_DRIVER_RS_TELEMETRY_ENABLED", "false")
        .env("CUA_DRIVER_RS_UPDATE_CHECK", "false")
        .env("QT_ACCESSIBILITY", "1")
        .env("ACCESSIBILITY_ENABLED", "1")
        .env_remove("NO_AT_BRIDGE")
}

fn detach(program: &str, args: &[&str], log: &str) -> Result<u32, Error> {
    let log = std::fs::File::create(format!("{STATE}/{log}.log"))?;
    let child = desktop_env(&mut Command::new(program))
        .args(args)
        .stdin(Stdio::null())
        .stdout(log.try_clone()?)
        .stderr(log)
        .process_group(0)
        .spawn()
        .map_err(|error| format!("could not start {program}: {error}"))?;
    Ok(child.id())
}

fn wait_for(what: &str, ready: impl Fn() -> bool) -> Result<(), Error> {
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(10) {
        if ready() {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    Err(format!("{what} did not start; see {STATE}/*.log").into())
}

fn listening(path: &str) -> bool {
    UnixStream::connect(path).is_ok()
}

fn alive(pid_file: &str) -> bool {
    std::fs::read_to_string(pid_file)
        .ok()
        .and_then(|pid| pid.trim().parse::<u32>().ok())
        .is_some_and(|pid| Path::new(&format!("/proc/{pid}")).exists())
}

/// Starts the virtual screen, the accessibility bus and the driver, once.
fn ensure_desktop() -> Result<String, Error> {
    if !Path::new(DRIVER).exists() {
        return Err("Computer use is not installed on this machine (no Cua Driver).".into());
    }
    std::fs::create_dir_all(STATE)?;
    std::fs::set_permissions(STATE, std::os::unix::fs::PermissionsExt::from_mode(0o700))?;
    if !listening(X_SOCKET) {
        detach(
            "Xvfb",
            &[DISPLAY, "-screen", "0", "1440x900x24", "-nolisten", "tcp"],
            "xvfb",
        )?;
        wait_for("The virtual screen (Xvfb)", || listening(X_SOCKET))?;
    }
    let bus_socket = format!("{STATE}/bus");
    if !listening(&bus_socket) {
        let address = bus();
        detach(
            "dbus-daemon",
            &[
                "--session",
                "--nofork",
                "--nopidfile",
                &format!("--address={address}"),
            ],
            "dbus",
        )?;
        wait_for("The session bus", || listening(&bus_socket))?;
    }
    let atspi = format!("{STATE}/atspi.pid");
    if !alive(&atspi) {
        let pid = detach(
            "/usr/libexec/at-spi-bus-launcher",
            &["--launch-immediately"],
            "atspi",
        )?;
        std::fs::write(&atspi, pid.to_string())?;
    }
    let socket = format!("{STATE}/driver.sock");
    if !listening(&socket) {
        let _ = std::fs::remove_file(&socket);
        detach(DRIVER, &["serve", "--socket", &socket], "driver")?;
        wait_for("Cua Driver", || listening(&socket))?;
    }
    Ok(socket)
}

fn call(tool: &str, json: Option<&String>) -> Result<i32, Error> {
    if BLOCKED_TOOLS.contains(&tool) {
        return Err(
            format!("{tool} changes Cua Driver itself and is not available to agents.").into(),
        );
    }
    let mut args: serde_json::Map<String, serde_json::Value> = match json {
        Some(text) => serde_json::from_str(text)
            .map_err(|_| "Tool arguments must be one JSON object, e.g. '{\"pid\":123}'.")?,
        None => serde_json::Map::new(),
    };
    if SESSION_TOOLS.contains(&tool) && !args.contains_key("session") {
        args.insert("session".into(), SESSION.into());
    }
    let wants_shot = matches!(tool, "get_window_state" | "get_desktop_state")
        && args.get("include_screenshot") != Some(&serde_json::Value::Bool(false));
    if wants_shot && !args.contains_key("screenshot_out_file") {
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis();
        std::fs::create_dir_all(format!("{STATE}/shots"))?;
        args.insert(
            "screenshot_out_file".into(),
            format!("{STATE}/shots/{tool}-{stamp}.png").into(),
        );
    }
    let socket = ensure_desktop()?;
    let status = desktop_env(&mut Command::new(DRIVER))
        .args([
            "call",
            "--socket",
            &socket,
            tool,
            &serde_json::Value::Object(args).to_string(),
        ])
        .status()?;
    Ok(status.code().unwrap_or(1))
}

pub fn cli(args: &[String]) -> Result<i32, Error> {
    match args.first().map(String::as_str) {
        Some("status") => {
            let installed = Path::new(DRIVER).exists();
            let running = listening(&format!("{STATE}/driver.sock"));
            println!(
                "{}",
                serde_json::json!({ "installed": installed, "running": running, "display": DISPLAY, "screenshots": format!("{STATE}/shots") })
            );
            Ok(0)
        }
        Some("start") => {
            ensure_desktop()?;
            println!(
                "{}",
                serde_json::json!({ "running": true, "display": DISPLAY })
            );
            Ok(0)
        }
        Some("launch") => {
            let program = args.get(1).ok_or(USAGE)?;
            ensure_desktop()?;
            let rest: Vec<&str> = args[2..].iter().map(String::as_str).collect();
            let pid = detach(program, &rest, "launch")?;
            println!("{}", serde_json::json!({ "pid": pid, "display": DISPLAY }));
            Ok(0)
        }
        Some("tools") => {
            let mut command = Command::new(DRIVER);
            match args.get(1) {
                Some(tool) => command.args(["describe", tool]),
                None => command.arg("list-tools"),
            };
            Ok(desktop_env(&mut command).status()?.code().unwrap_or(1))
        }
        Some("call") => call(args.get(1).ok_or(USAGE)?, args.get(2)),
        _ => Err(USAGE.into()),
    }
}
