// Copyright © 2019 VMware, Inc. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0 OR MIT

use crate::Benchmark;
use plotters::prelude::*;

pub fn plot_throughput_scale_out(
    results: &Vec<(u64, u64)>,
) -> Result<(), Box<dyn std::error::Error>> {
    let root = BitMapBackend::new("threads_throughput.png", (1024, 768)).into_drawing_area();
    root.fill(&White)?;

    let max_y = *results.iter().map(|(_ts, tput)| tput).max().unwrap_or(&0);
    let max_x = *results.iter().map(|(ts, _tput)| ts).max().unwrap_or(&1);

    let mut chart = ChartBuilder::on(&root)
        .caption(
            "Stack throughput (vary threads)",
            ("Supria Sans", 23).into_font(),
        )
        .margin(20)
        .x_label_area_size(50)
        .y_label_area_size(100)
        .build_ranged(1..(max_x + 1), 0u64..max_y)?;

    chart
        .configure_mesh()
        .x_label_offset(0)
        .x_labels(28)
        .y_desc("Throughput [ops/s]")
        .x_desc("Threads")
        .axis_desc_style(("Supria Sans", 24).into_font())
        .label_style(("Decima Mono", 15).into_font())
        .draw()?;

    chart.draw_series(LineSeries::new(results.clone(), &Red))?;

    chart.draw_series(PointSeries::of_element(
        results.clone(),
        5,
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

pub fn plot_per_thread_throughput(
    c: &Benchmark,
    results: &Vec<(u64, u64, i64)>,
) -> Result<(), Box<dyn std::error::Error>> {
    let filename = format!(
        "thread_throughputs_r_{}_t_{}_l_{}_n_{}.png",
        c.r, c.t, c.l, c.n
    );
    let max_y = *results
        .iter()
        .map(|(_ts, tput, _dur)| tput)
        .max()
        .unwrap_or(&0);

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
        .build_ranged(0u64..c.t as u64, 0u64..max_y)?;

    chart
        .configure_mesh()
        .disable_x_mesh()
        .line_style_1(&White.mix(0.3))
        .x_label_offset(37)
        .x_labels(c.t)
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
