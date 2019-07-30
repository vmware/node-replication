extern crate chrono;
extern crate clap;
extern crate core_affinity;
extern crate csv;
extern crate nom;
extern crate rand;
extern crate rustc_serialize;
extern crate std;

mod pinning;

use std::cell::{Cell, RefCell};
use std::sync::{Arc, Barrier};
use std::thread;
use std::usize;

use clap::{load_yaml, App};

use chrono::Duration;

use node_replication::log::Log;
use node_replication::replica::Replica;
use node_replication::Dispatch;

use rand::{thread_rng, Rng};

use plotters::prelude::*;

const DEFAULT_STACK_SIZE: u32 = 1000u32 * 1000u32;

#[derive(Clone, Copy)]
enum Op {
    Push(u32),

    Pop,

    Invalid,
}

impl Default for Op {
    fn default() -> Op {
        Op::Invalid
    }
}

struct Stack {
    storage: RefCell<Vec<u32>>,
    sum: Cell<u32>,
}

impl Stack {
    pub fn push(&self, data: u32) {
        self.storage.borrow_mut().push(data);
    }

    pub fn pop(&self) -> Option<u32> {
        let r = self.storage.borrow_mut().pop();

        if r.is_none() {
            return Some(0);
        }

        r
    }
}

impl Default for Stack {
    fn default() -> Stack {
        let s = Stack {
            storage: Default::default(),
            sum: Default::default(),
        };

        for e in 0..DEFAULT_STACK_SIZE {
            s.push(e);
        }

        s
    }
}

impl Dispatch for Stack {
    type Operation = Op;

    fn dispatch(&self, op: Self::Operation) {
        match op {
            Op::Push(v) => self.push(v),

            Op::Pop => {
                self.sum.set(self.pop().unwrap());
            }

            Op::Invalid => unreachable!("Op::Invalid?"),
        }
    }
}

/// TODO: Verify Linearizability for the stack.
/// TODO: Correctness test for the stack.
fn bench(
    r: Arc<Replica<Stack>>,
    nop: usize,
    barrier: Arc<Barrier>,
    core: pinning::Core,
) -> (u64, u64) {
    core_affinity::set_for_current(core_affinity::CoreId { id: core as usize });

    let idx = r.register().expect("Failed to register with Replica.");

    let mut orng = thread_rng();
    let mut arng = thread_rng();

    let mut ops = Vec::with_capacity(nop);
    for _i in 0..nop {
        let op: usize = orng.gen();
        match op % 2usize {
            0usize => ops.push(Op::Pop),
            1usize => ops.push(Op::Push(arng.gen())),
            _ => ops.push(Op::Invalid),
        }
    }
    barrier.wait();

    let time = Duration::span(|| {
        for i in 0..nop {
            r.execute(ops[i], idx);
        }
    });

    let throughput: usize = (nop * 1000 * 1000) / (time.num_microseconds().unwrap()) as usize;
    println!("Thread {} Throughput: {} op/s", idx, throughput);

    (idx as u64, throughput as u64)
}

struct Config {
    r: usize,
    t: usize,
    l: usize,
    n: usize,
}

fn main() {
    let yml = load_yaml!("args.yml");
    let matches = App::from_yaml(yml).get_matches();

    let r = usize::from_str_radix(matches.value_of("replicas").unwrap(), 10).unwrap();
    let ts: Vec<&str> = matches.values_of("threads").unwrap().collect();
    let ts: Vec<usize> = ts
        .iter()
        .map(|t| usize::from_str_radix(t, 10).unwrap_or(1))
        .collect();
    let l = usize::from_str_radix(matches.value_of("logsz").unwrap(), 10).unwrap();
    let n = usize::from_str_radix(matches.value_of("nop").unwrap(), 10).unwrap();

    let m = pinning::MachineTopology::new();
    let s = m.sockets();

    if r > s.len() {
        panic!("Requested for more replicas than sockets on this machine!");
    }

    let mut tputs: Vec<(u64, u64)> = Vec::with_capacity(ts.len());
    for t in ts {
        let c = Config { r, t, l, n };

        let log = Arc::new(Log::<<Stack as Dispatch>::Operation>::new(
            l * 1024 * 1024 * 1024,
        ));

        let mut replicas = Vec::with_capacity(r);
        for _i in 0..r {
            replicas.push(Arc::new(Replica::<Stack>::new(&log)));
        }

        let mut threads = Vec::new();
        let barrier = Arc::new(Barrier::new(t * r));

        for i in 0..r {
            let cores = m.cores_on_socket(s[i]);
            if t > cores.len() {
                panic!("Requested for more threads than cores on this socket!");
            }

            for j in 0..t {
                let r = replicas[i].clone();
                let o = n.clone();
                let b = barrier.clone();
                let c = cores[j];
                let child = thread::spawn(move || bench(r, o, b, c));
                threads.push(child);
            }
        }

        let mut results: Vec<(u64, u64)> = Vec::with_capacity(c.t);
        for _i in 0..threads.len() {
            let retval = threads
                .pop()
                .unwrap()
                .join()
                .expect("Thread didn't finish successfully.");
            results.push(retval);
        }
        plot_per_thread_throughput(c, &results).unwrap();
        tputs.push((
            results.len() as u64,
            results.iter().map(|(_rid, tput)| tput).sum(),
        ));
    }

    plot_throughput_scale_out(&tputs).expect("Can't plot throughput scale-out graph");
}

fn plot_throughput_scale_out(results: &Vec<(u64, u64)>) -> Result<(), Box<dyn std::error::Error>> {
    let root = BitMapBackend::new("threads_throughput.png", (1024, 768)).into_drawing_area();
    root.fill(&White)?;

    let max_y = *results.iter().map(|(_ts, tput)| tput).max().unwrap_or(&0);
    let max_x = *results.iter().map(|(ts, _tput)| ts).max().unwrap_or(&1);

    let mut chart = ChartBuilder::on(&root)
        .caption(
            "Stack throughput (vary threads)",
            ("Supria Sans", 23).into_font(),
        )
        .margin(5)
        .x_label_area_size(120)
        .y_label_area_size(100)
        .build_ranged(1..(max_x + 1), 0u64..max_y)?;

    chart
        .configure_mesh()
        .x_label_offset(0)
        .x_labels(24)
        .y_desc("Throughput [ops/s]")
        .x_desc("Threads")
        .axis_desc_style(("Supria Sans", 26).into_font())
        .label_style(("Decima Mono", 15).into_font())
        .draw()?;

    chart.draw_series(LineSeries::new(results.clone(), &Red))?;

    chart.draw_series(PointSeries::of_element(
        results.clone(),
        results.len() as u32,
        ShapeStyle::from(&Red).filled(),
        &|coord, size, style| {
            EmptyElement::at(coord)
                + Circle::new((0, 0), size, style)
                + Text::new(
                    format!("{:.2} M", (coord.1 as f64 / 1e6)),
                    (0, 15),
                    ("Decima Mono", 15).into_font(),
                )
        },
    ))?;

    Ok(())
}

fn plot_per_thread_throughput(
    c: Config,
    results: &Vec<(u64, u64)>,
) -> Result<(), Box<dyn std::error::Error>> {
    let filename = format!(
        "thread_throughputs_r_{}_t_{}_l_{}_n_{}.png",
        c.r, c.t, c.l, c.n
    );
    let root = BitMapBackend::new(filename.as_str(), (1024, 768)).into_drawing_area();

    root.fill(&White)?;

    let mut chart = ChartBuilder::on(&root)
        .x_label_area_size(45)
        .y_label_area_size(100)
        .margin(5)
        .caption(
            format!("Throughput t={}", c.t),
            ("Supria Sans", 23.0).into_font(),
        )
        .build_ranged(
            0u64..std::cmp::max(c.r as u64, (c.r * c.t) as u64),
            0u64..700000u64,
        )?;

    chart
        .configure_mesh()
        .disable_x_mesh()
        .line_style_1(&White.mix(0.3))
        .x_label_offset(37)
        .x_labels(c.r * c.t)
        .y_desc("Throughput [ops/s]")
        .x_desc("Thread ID")
        .axis_desc_style(("Supria Sans", 26).into_font())
        .label_style(("Decima Mono", 15).into_font())
        .draw()?;

    let mut tputs: Vec<(u64, u64)> = Vec::with_capacity(c.t);
    for i in 0..results.len() {
        tputs.push((i as u64, results[i].1));
    }

    chart.draw_series(
        Histogram::vertical(&chart)
            .style(Red.mix(0.5).filled())
            .data(tputs),
    )?;

    Ok(())
}
