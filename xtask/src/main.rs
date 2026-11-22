//! 發布工具，以 `cargo xtask <命令>` 執行。用 Rust 寫而不是 shell 腳本：可測試、跨平台、錯誤處理一致。
//!
//! - `build-record --binary <檔> --target <三元組> --out <json>`：產生建置記錄
//!   （比照 vm2api `docs/releases/codex-kernel-v0.1.2.json` 的欄位）。
//! - `sha256sums <檔>...`：輸出 `SHA256SUMS` 格式。
//! - `check-glibc --binary <檔> --max <版本>`：二進位需要的 glibc 不得高於上限。

// 這是命令列工具，輸出就是結果。
#![allow(clippy::print_stdout)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, bail};
use serde_json::json;
use sha2::{Digest, Sha256};

fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let command = args.next().unwrap_or_default();
    let rest: Vec<String> = args.collect();
    match command.as_str() {
        "build-record" => build_record(&flags(&rest)?),
        "sha256sums" => sha256sums(&rest),
        "check-glibc" => check_glibc(&flags(&rest)?),
        _ => bail!("usage: cargo xtask <build-record|sha256sums|check-glibc> ..."),
    }
}

fn flags(args: &[String]) -> anyhow::Result<BTreeMap<String, String>> {
    let mut out = BTreeMap::new();
    let mut iter = args.iter();
    while let Some(key) = iter.next() {
        let Some(name) = key.strip_prefix("--") else {
            bail!("unexpected argument {key}");
        };
        let value = iter.next().with_context(|| format!("--{name} needs a value"))?;
        out.insert(name.to_owned(), value.clone());
    }
    Ok(out)
}

fn flag<'a>(flags: &'a BTreeMap<String, String>, name: &str) -> anyhow::Result<&'a str> {
    flags
        .get(name)
        .map(String::as_str)
        .with_context(|| format!("missing --{name}"))
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

fn command_output(program: &str, args: &[&str]) -> anyhow::Result<String> {
    let output = Command::new(program)
        .args(args)
        .output()
        .with_context(|| format!("run {program}"))?;
    if !output.status.success() {
        bail!("{program} {} failed", args.join(" "));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

/// 從 ELF 的版本字串找出需要的最高 glibc 版本（例如 `GLIBC_2.34`）。
/// 版本需求字串存在動態字串表中，直接掃描位元組即可，不需要完整解析 ELF。
fn max_glibc(bytes: &[u8]) -> Option<(u32, u32)> {
    const PREFIX: &[u8] = b"GLIBC_";
    let mut best: Option<(u32, u32)> = None;
    let mut index = 0;
    while let Some(offset) = bytes[index..].windows(PREFIX.len()).position(|w| w == PREFIX) {
        let start = index + offset + PREFIX.len();
        let end = bytes[start..]
            .iter()
            .position(|b| !(b.is_ascii_digit() || *b == b'.'))
            .map_or(bytes.len(), |p| start + p);
        if let Ok(text) = std::str::from_utf8(&bytes[start..end]) {
            let mut parts = text.split('.');
            if let (Some(Ok(major)), Some(Ok(minor))) =
                (parts.next().map(str::parse), parts.next().map(str::parse))
            {
                best = best.max(Some((major, minor)));
            }
        }
        index = start;
    }
    best
}

fn parse_version(text: &str) -> anyhow::Result<(u32, u32)> {
    let mut parts = text.split('.');
    match (parts.next().map(str::parse), parts.next().map(str::parse)) {
        (Some(Ok(major)), Some(Ok(minor))) => Ok((major, minor)),
        _ => bail!("invalid version {text}"),
    }
}

fn check_glibc(flags: &BTreeMap<String, String>) -> anyhow::Result<()> {
    let binary = PathBuf::from(flag(flags, "binary")?);
    let max = parse_version(flag(flags, "max")?)?;
    let bytes = std::fs::read(&binary).with_context(|| format!("read {}", binary.display()))?;
    match max_glibc(&bytes) {
        Some(found) if found > max => bail!(
            "{} needs glibc {}.{}, above the {}.{} ceiling",
            binary.display(),
            found.0,
            found.1,
            max.0,
            max.1
        ),
        Some(found) => println!(
            "{}: glibc {}.{} (ceiling {}.{})",
            binary.display(),
            found.0,
            found.1,
            max.0,
            max.1
        ),
        None => println!("{}: no glibc version requirement", binary.display()),
    }
    Ok(())
}

fn sha256sums(files: &[String]) -> anyhow::Result<()> {
    if files.is_empty() {
        bail!("usage: cargo xtask sha256sums <file>...");
    }
    for file in files {
        let path = Path::new(file);
        let bytes = std::fs::read(path).with_context(|| format!("read {file}"))?;
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or(file);
        println!("{}  {name}", sha256_hex(&bytes));
    }
    Ok(())
}

fn build_record(flags: &BTreeMap<String, String>) -> anyhow::Result<()> {
    let binary = PathBuf::from(flag(flags, "binary")?);
    let target = flag(flags, "target")?;
    let out = PathBuf::from(flag(flags, "out")?);
    let bytes = std::fs::read(&binary).with_context(|| format!("read {}", binary.display()))?;
    let lock = std::fs::read("Cargo.lock").context("read Cargo.lock")?;
    let dirty = !command_output("git", &["status", "--porcelain"])?.is_empty();
    let glibc = max_glibc(&bytes).map(|(major, minor)| format!("{major}.{minor}"));
    let record = json!({
        "component": "tap-codex-kernel",
        "version": env!("CARGO_PKG_VERSION"),
        "source": {
            "visibility": std::env::var("TAP_SOURCE_VISIBILITY").ok(),
            "repository": "holeenlu/tap-codex-kernel",
            "commit": command_output("git", &["rev-parse", "HEAD"])?,
            "tree": command_output("git", &["rev-parse", "HEAD^{tree}"])?,
            "working_tree_changes_included": dirty,
            "cargo_lock_sha256": sha256_hex(&lock),
        },
        "toolchain": {
            "rustc": command_output("rustc", &["-V"])?,
            "cargo": command_output("cargo", &["-V"])?,
            "target": target,
        },
        "build": {
            "profile": "release",
            "locked": true,
            "strip": "symbols",
            "source_paths_remapped": true,
        },
        "artifact": {
            "name": binary.file_name().and_then(|n| n.to_str()).unwrap_or("tap-codex-kernel"),
            "size": bytes.len(),
            "sha256": sha256_hex(&bytes),
            "max_required_glibc": glibc,
        },
    });
    if dirty {
        bail!("refusing to record a build from a dirty working tree");
    }
    let text = serde_json::to_string_pretty(&record)?;
    std::fs::write(&out, format!("{text}\n")).with_context(|| format!("write {}", out.display()))?;
    println!("{}", out.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_the_highest_glibc_requirement() {
        let blob = b"\0GLIBC_2.17\0GLIBC_2.34\0GLIBC_PRIVATE\0GLIBC_2.2.5\0";
        assert_eq!(max_glibc(blob), Some((2, 34)));
        assert_eq!(max_glibc(b"no requirement here"), None);
    }

    #[test]
    fn hashes_like_sha256sum() {
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn parses_versions() {
        assert_eq!(parse_version("2.34").unwrap(), (2, 34));
        assert!(parse_version("two").is_err());
    }
}
