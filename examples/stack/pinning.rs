use nom::*;

use std::process::Command;
use std::str::{from_utf8_unchecked, FromStr};

pub type Node = u64;
pub type Socket = u64;
pub type Core = u64;
pub type Cpu = u64;
pub type L1 = u64;
pub type L2 = u64;
pub type L3 = u64;

fn to_string(s: &[u8]) -> &str {
    unsafe { from_utf8_unchecked(s) }
}

fn to_u64(s: &str) -> u64 {
    FromStr::from_str(s).unwrap()
}

fn buf_to_u64(s: &[u8]) -> u64 {
    to_u64(to_string(s))
}

named!(parse_numactl_size<&[u8], NodeInfo>,
    chain!(
        tag!("node") ~
        take_while!(is_space) ~
        node: take_while!(is_digit) ~
        take_while!(is_space) ~
        tag!("size:") ~
        take_while!(is_space) ~
        size: take_while!(is_digit) ~
        take_while!(is_space) ~
        tag!("MB"),
        || NodeInfo { node: buf_to_u64(node), memory: buf_to_u64(size) * 1000000 }
    )
);

fn get_node_info(node: Node, numactl_output: &String) -> Option<NodeInfo> {
    let find_prefix = format!("node {} size:", node);
    for line in numactl_output.split('\n') {
        if line.starts_with(find_prefix.as_str()) {
            let res = parse_numactl_size(line.as_bytes());
            return Some(res.unwrap().1);
        }
    }

    None
}

#[derive(Debug, Eq, PartialEq, RustcEncodable)]
pub struct CpuInfo {
    pub node: NodeInfo,
    pub socket: Socket,
    pub core: Core,
    pub cpu: Cpu,
    pub l1: L1,
    pub l2: L2,
    pub l3: L3,
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Copy, Clone, RustcEncodable)]
pub struct NodeInfo {
    pub node: Node,
    pub memory: u64,
}

#[derive(Debug)]
pub struct MachineTopology {
    data: Vec<CpuInfo>,
}

impl MachineTopology {
    pub fn new() -> MachineTopology {
        let lscpu_out = Command::new("lscpu")
            .arg("--parse=NODE,SOCKET,CORE,CPU,CACHE")
            .output()
            .unwrap();
        let lscpu_string = String::from_utf8(lscpu_out.stdout).unwrap_or(String::new());

        let numactl_out = Command::new("numactl").arg("--hardware").output().unwrap();
        let numactl_string = String::from_utf8(numactl_out.stdout).unwrap_or(String::new());

        MachineTopology::from_strings(lscpu_string, numactl_string)
    }

    pub fn from_strings(lscpu_output: String, numactl_output: String) -> MachineTopology {
        let no_comments: Vec<&str> = lscpu_output
            .split('\n')
            .filter(|s| s.trim().len() > 0 && !s.trim().starts_with("#"))
            .collect();

        type Row = (Node, Socket, Core, Cpu, String); // Online MHz
        let mut rdr = csv::Reader::from_string(no_comments.join("\n")).has_headers(false);
        let rows = rdr.decode().collect::<csv::Result<Vec<Row>>>().unwrap();

        let mut data: Vec<CpuInfo> = Vec::with_capacity(rows.len());
        for row in rows {
            let caches: Vec<u64> = row
                .4
                .split(":")
                .map(|s| u64::from_str(s).unwrap())
                .collect();
            assert_eq!(caches.len(), 4);
            let node: NodeInfo =
                get_node_info(row.0, &numactl_output).expect("Can't find node in numactl output?");
            let tuple: CpuInfo = CpuInfo {
                node: node,
                socket: row.1,
                core: row.2,
                cpu: row.3,
                l1: caches[0],
                l2: caches[2],
                l3: caches[3],
            };
            data.push(tuple);
        }

        MachineTopology { data: data }
    }

    pub fn sockets(&self) -> Vec<Socket> {
        let mut sockets: Vec<Cpu> = self.data.iter().map(|t| t.socket).collect();
        sockets.sort();
        sockets.dedup();
        sockets
    }

    pub fn cores_on_socket(&self, socket: Socket) -> Vec<Core> {
        let mut cores: Vec<Core> = self
            .data
            .iter()
            .filter(|c| c.socket == socket)
            .map(|c| c.core)
            .collect();
        cores.sort();
        cores.dedup();
        cores
    }
}
