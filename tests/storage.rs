use cloudroom::workspace::storage::Policy;
use std::{
    fs,
    time::{SystemTime, UNIX_EPOCH},
};

#[test]
fn storage_requires_protected_explicit_policy() {
    let path = std::env::temp_dir().join(format!(
        "cloudroom-policy-{}-{}.json",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::write(&path, r#"{"agent_uid":1001,"agent_gid":1001,"quota_mount":"/","quota_limit_bytes":1048576,"cache_dir":"/var/cache/cloudroom-agent","cgroup_root":"/sys/fs/cgroup/example/agents"}"#).unwrap();
    // /tmp is writable by agents: a policy placed here cannot establish trusted limits.
    assert!(Policy::load(&path).is_err());
    fs::remove_file(path).unwrap();
}

#[test]
fn installer_rejects_unsafe_accounts_before_provisioning() {
    // Run the real installer entry point, but stop at the first filesystem probe.
    // No root access, quota changes, or host configuration writes are possible.
    let probe = r#"
        id() {
            case "$1:$#" in
                -u:1) printf '%s\n' "$CALLER_UID" ;;
                -u:2) printf '%s\n' "$AGENT_UID" ;;
                -g:2|-G:2) printf '1001\n' ;;
                *) return 1 ;;
            esac
        }
        findmnt() {
            printf 'reached filesystem provisioning\n' >&2
            printf 'unsupported-test-filesystem\n'
        }
        export -f id findmnt
        exec /bin/bash "$@"
    "#;
    for (caller, uid, service, error, reaches_probe) in [
        ("0", "0", "service", "Agent account must not be root", false),
        (
            "0",
            "1001",
            "agent",
            "Agent and service accounts must be different",
            false,
        ),
        (
            "1001",
            "1001",
            "service",
            "Run as provisioning administrator",
            false,
        ),
        (
            "0",
            "1001",
            "service",
            "Only verified ext4 root quotas are supported",
            true,
        ),
    ] {
        let output = std::process::Command::new("/bin/bash")
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("CALLER_UID", caller)
            .env("AGENT_UID", uid)
            .args(["-c", probe, "storage-installer-test"])
            .arg(concat!(env!("CARGO_MANIFEST_DIR"), "/install/storage.sh"))
            .args(["agent", service, "/unused-test-policy.json"])
            .output()
            .unwrap();
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert!(!output.status.success(), "installer unexpectedly succeeded");
        assert!(
            stderr.contains(error),
            "caller={caller}, agent={uid}, service={service}: {stderr}"
        );
        assert_eq!(
            stderr.contains("reached filesystem provisioning"),
            reaches_probe,
            "{stderr}"
        );
    }
}
