"""
Script to generate the comprehensive, plain-English PDF guide for NetScope:
'NETSCOPE_PLAIN_ENGLISH_EXPLANATION.pdf'

No buzzwords. No corporate hype. Explains what every single tool and feature is,
and why it was added to the project.
"""

from __future__ import annotations

import os
import sys
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
    KeepTogether,
    PageBreak,
    HRFlowable,
)
from reportlab.pdfgen import canvas


class NumberedCanvas(canvas.Canvas):
    """Two-pass canvas that dynamically counts total pages and draws headers/footers."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._saved_page_states = []

    def showPage(self):
        self._saved_page_states.append(dict(self.__dict__))
        self._startPage()

    def save(self):
        num_pages = len(self._saved_page_states)
        for state in self._saved_page_states:
            self.__dict__.update(state)
            self.draw_header_footer(num_pages)
            super().showPage()
        super().save()

    def draw_header_footer(self, page_count: int):
        self.saveState()
        self.setFont("Helvetica", 8)
        self.setFillColor(colors.HexColor("#64748b"))

        # Do not draw header on the cover page (Page 1)
        if self._pageNumber > 1:
            self.drawString(54, 750, "NetScope — Plain-English Feature & Architecture Guide")
            self.drawRightString(558, 750, "No-Buzzwords Edition")
            self.setStrokeColor(colors.HexColor("#cbd5e1"))
            self.setLineWidth(0.5)
            self.line(54, 744, 558, 744)

        # Footer on all pages
        self.setStrokeColor(colors.HexColor("#cbd5e1"))
        self.setLineWidth(0.5)
        self.line(54, 45, 558, 45)
        self.drawString(54, 32, "NetScope Distributed Internet Observatory · Practical Data Engineering")
        self.drawRightString(558, 32, f"Page {self._pageNumber} of {page_count}")
        self.restoreState()


def create_callout(text: str, title: str = "", bg_color="#f1f5f9", border_color="#94a3b8") -> Table:
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "CalloutTitle",
        parent=styles["Normal"],
        fontName="Helvetica-Bold",
        fontSize=10,
        leading=13,
        textColor=colors.HexColor("#1e293b"),
    )
    body_style = ParagraphStyle(
        "CalloutBody",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=9,
        leading=13,
        textColor=colors.HexColor("#334155"),
    )

    contents = []
    if title:
        contents.append(Paragraph(title, title_style))
        contents.append(Spacer(1, 4))
    contents.append(Paragraph(text, body_style))

    tbl = Table([[contents]], colWidths=[504])
    tbl.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor(bg_color)),
        ("BOX", (0, 0), (-1, -1), 0.75, colors.HexColor(border_color)),
        ("TOPPADDING", (0, 0), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
        ("LEFTPADDING", (0, 0), (-1, -1), 12),
        ("RIGHTPADDING", (0, 0), (-1, -1), 12),
    ]))
    return tbl


def generate_pdf(output_path: str):
    doc = SimpleDocTemplate(
        output_path,
        pagesize=letter,
        leftMargin=54,
        rightMargin=54,
        topMargin=58,
        bottomMargin=54,
    )

    styles = getSampleStyleSheet()

    # Custom styles
    title_style = ParagraphStyle(
        "DocTitle",
        parent=styles["Normal"],
        fontName="Helvetica-Bold",
        fontSize=24,
        leading=28,
        textColor=colors.HexColor("#0f172a"),
        spaceAfter=6,
    )

    subtitle_style = ParagraphStyle(
        "DocSubtitle",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=12,
        leading=16,
        textColor=colors.HexColor("#475569"),
        spaceAfter=14,
    )

    meta_badge_style = ParagraphStyle(
        "MetaBadge",
        parent=styles["Normal"],
        fontName="Helvetica-Bold",
        fontSize=9,
        leading=12,
        textColor=colors.HexColor("#1e40af"),
    )

    h1_style = ParagraphStyle(
        "Header1",
        parent=styles["Normal"],
        fontName="Helvetica-Bold",
        fontSize=15,
        leading=18,
        textColor=colors.HexColor("#0f172a"),
        spaceBefore=14,
        spaceAfter=6,
        keepWithNext=True,
    )

    h2_style = ParagraphStyle(
        "Header2",
        parent=styles["Normal"],
        fontName="Helvetica-Bold",
        fontSize=11,
        leading=15,
        textColor=colors.HexColor("#1e40af"),
        spaceBefore=10,
        spaceAfter=4,
        keepWithNext=True,
    )

    body_style = ParagraphStyle(
        "BodyDark",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=9.5,
        leading=13.5,
        textColor=colors.HexColor("#1e293b"),
        spaceAfter=6,
    )

    bullet_style = ParagraphStyle(
        "BulletText",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=9,
        leading=13,
        textColor=colors.HexColor("#334155"),
        leftIndent=15,
        firstLineIndent=-10,
        spaceAfter=3,
    )

    story = []

    # Cover Title Block
    story.append(Paragraph("NetScope — Plain English System Guide", title_style))
    story.append(Paragraph("What every single feature is, how it works, and why we added it — without any buzzwords or technical jargon.", subtitle_style))
    
    badge_table = Table(
        [[Paragraph("<b>Audience:</b> Anyone wanting to understand the engineering without hype &nbsp;|&nbsp; <b>Project:</b> NetScope Internet Observatory &nbsp;|&nbsp; <b>Stack:</b> Python, Docker, Kafka, Flink, MinIO, ClickHouse", meta_badge_style)]],
        colWidths=[504]
    )
    badge_table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#e0e7ff")),
        ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor("#93c5fd")),
        ("PADDING", (0, 0), (-1, -1), 6),
    ]))
    story.append(badge_table)
    story.append(Spacer(1, 14))

    # Section 1: The Big Picture
    story.append(Paragraph("1. The Big Picture: What Does This System Actually Do?", h1_style))
    story.append(Paragraph(
        "Imagine you run an online business or rely on websites like Google, GitHub, and Cloudflare. One morning, users complain that your app is loading slowly. Is the entire website down for everyone in the world? Is your local internet company having cable trouble? Did someone's Wi-Fi router crash? Or is the website's server taking too long to think?",
        body_style
    ))
    story.append(Paragraph(
        "Normally, people guess, argue, or check Twitter to see if anyone else is complaining. <b>NetScope replaces guessing with proof.</b>",
        body_style
    ))
    story.append(Paragraph(
        "We built an automated system that places small measurement programs ('watchers' or 'probes') in different cities around the world (Mumbai, Singapore, Frankfurt). Every few seconds, these watchers visit important websites, measure every single millisecond of the connection, push that data through a reliable data pipeline, detect slowdowns using simple mathematics, and alert engineers before customers get angry.",
        body_style
    ))
    story.append(Spacer(1, 6))

    # Section 2: Foundational Tools (Docker & Kafka)
    story.append(Paragraph("2. The Foundational Tools: What Are They & Why Did We Add Them?", h1_style))
    story.append(Paragraph("If you've never used tools like Docker or Kafka before, here is what each tool actually is in plain everyday terms:", body_style))

    # Docker
    story.append(Paragraph("A. Docker & Docker Compose (The Clean Shipping Containers)", h2_style))
    story.append(Paragraph(
        "<b>What it is:</b> Imagine you buy a complicated refrigerator with 20 electrical wires. If you have to wire it into your house walls yourself, one mistake can cause a short circuit or conflict with other appliances. A shipping container puts the refrigerator, its own battery, and all its wiring into a neat box that you simply plug in with one cord. That is Docker. It packages software into isolated 'containers' so it works identically on any computer.",
        body_style
    ))
    story.append(Paragraph(
        "<b>Why we added it:</b> Our platform needs five different supporting programs (a database, a message queue, a storage server, and two monitoring dashboards). Without Docker, you would have to spend days downloading separate Windows installers, configuring ports, and debugging missing files. With Docker Compose, a single text file (<code>docker-compose.yml</code>) turns on all five programs in 10 seconds with one command.",
        body_style
    ))

    # Probes
    story.append(Paragraph("B. The Probes (The Geographic Watchers)", h2_style))
    story.append(Paragraph(
        "<b>What it is:</b> A probe is just a small, fast Python program that runs continuously. It holds a list of websites (like <code>google.com</code>, <code>github.com</code>, <code>cloudflare.com</code>) and visits them every few seconds.",
        body_style
    ))
    story.append(Paragraph(
        "<b>Why we added it:</b> If you test a website only from your own laptop in your living room, you only know if your own laptop can reach it. By running separate probes tagged with geographic locations (India, Singapore, Europe), we can see if a website is slow everywhere on earth, or only slow in one specific country.",
        body_style
    ))

    # Protocol Breakdown
    story.append(Paragraph("C. Protocol Timing Breakdown (DNS, TCP, TLS, TTFB)", h2_style))
    story.append(Paragraph(
        "When your computer loads a webpage, four distinct things happen in order. Our probes measure the exact duration of each one:",
        body_style
    ))
    story.append(Paragraph("• <b>DNS Lookup (The Phonebook):</b> Before contacting <code>google.com</code>, your computer must look up Google's numeric address (like <code>142.250.190.46</code>). This measures how long it takes to find the number in the internet phonebook.", bullet_style))
    story.append(Paragraph("• <b>TCP Connection (The Knock on the Door):</b> Your computer reaches out across the physical cables to the server and waits for the server to answer 'hello, I am ready to talk'. This measures network travel time.", bullet_style))
    story.append(Paragraph("• <b>TLS Handshake (The Secret Code):</b> To ensure nobody can spy on your password or credit card, the two computers exchange encryption keys (creating the HTTPS padlock). This measures the time spent setting up security.", bullet_style))
    story.append(Paragraph("• <b>TTFB / Time to First Byte (The Server's Brain):</b> Your computer asks for the webpage, and the server's computer has to search its files or run code before sending the first byte of data back. This measures how fast the server's brain works.", bullet_style))
    story.append(Paragraph(
        "<b>Why we added this:</b> If a webpage takes 2 seconds to load, knowing only 'it took 2 seconds' doesn't help engineers fix it. If the DNS lookup took 1.9 seconds, the phonebook server is broken. If the TCP connection took 1.9 seconds, the physical network cable is choked. If TTFB took 1.9 seconds, the server's code is slow. Splitting the time tells engineers exactly who to call.",
        body_style
    ))

    story.append(Spacer(1, 10))

    # Section 3: The Data Pipeline (Kafka, Flink, MinIO, ClickHouse)
    story.append(Paragraph("3. The Data Pipeline: Moving & Saving Millions of Measurements", h1_style))

    # Kafka
    story.append(Paragraph("A. Apache Kafka (The Unbreakable Conveyor Belt)", h2_style))
    story.append(Paragraph(
        "<b>What it is:</b> Think of Kafka as a heavy-duty factory conveyor belt. When our probes take measurements every second, hundreds of readings arrive constantly. Probes place measurements on one end of the conveyor belt, and other programs take them off the other end.",
        body_style
    ))
    story.append(Paragraph(
        "<b>Why we added it:</b> If you try to save every reading directly into a database the exact millisecond it happens, the database can freeze up under heavy traffic and crash. When a database crashes, new readings vanish into thin air. Kafka prevents this: even if the database is turned off for maintenance, Kafka safely holds all incoming measurements on the conveyor belt without dropping a single one. When the database comes back online, it reads through the backlog and catches up. In our stress tests, Kafka swallowed <b>9,250 measurements every single second</b> without breaking a sweat.",
        body_style
    ))

    # Flink
    story.append(Paragraph("B. Apache Flink Stream Engine (The Live Calculator)", h2_style))
    story.append(Paragraph(
        "<b>What it is:</b> While Kafka moves the data, Flink is a calculator sitting right next to the conveyor belt, inspecting every measurement as it flies past in real time.",
        body_style
    ))
    story.append(Paragraph(
        "<b>Why we added it:</b> You don't want to wait until tomorrow to calculate today's average website speed. Flink groups incoming readings into 1-minute and 5-minute rolling 'time windows'. It immediately calculates the median (50th percentile) and worst-case (95th and 99th percentile) speeds. It also handles two tricky real-world problems:",
        body_style
    ))
    story.append(Paragraph("• <b>Duplicates:</b> If a probe sends the exact same reading twice due to a Wi-Fi hiccup, Flink remembers recent IDs and discards the duplicate.", bullet_style))
    story.append(Paragraph("• <b>Late Events & Watermarks:</b> If an internet packet gets stuck in traffic and arrives 15 seconds late, Flink's 'watermark' gives it a short grace period. If it arrives way too late after the 1-minute window was already closed, Flink routes it to a separate 'late box' (Dead Letter Queue) so it doesn't corrupt clean statistics.", bullet_style))

    # MinIO & Parquet
    story.append(Paragraph("C. MinIO & Parquet Format (The Cheap, Permanent Filing Cabinet)", h2_style))
    story.append(Paragraph(
        "<b>What it is:</b> MinIO is private cloud file storage (just like Amazon S3) running directly on your computer. Inside MinIO, we save raw measurement files organized neatly by date (e.g. <code>year=2026/month=11/day=16/probe=india-01/part-001.parquet</code>).",
        body_style
    ))
    story.append(Paragraph(
        "<b>Why we added Parquet:</b> Most people save data as plain text (CSV or JSON). In our measured shootout on 50,000 measurements:",
        body_style
    ))
    
    shootout_data = [
        ["File Format", "File Size on Disk", "Read Speed", "Speed to Read 2 Columns", "Space Savings"],
        ["JSON Lines (JSONL)", "16.77 MB", "84,746 rows / sec", "589.65 ms", "Baseline (1.0x)"],
        ["CSV (Comma-Separated)", "6.85 MB", "284,091 rows / sec", "176.32 ms", "2.4x smaller"],
        ["Parquet (Snappy)", "2.67 MB", "1,221,729 rows / sec", "4.90 ms", "6.3x smaller (84% saved)"]
    ]
    t_shootout = Table(shootout_data, colWidths=[110, 95, 105, 114, 80])
    t_shootout.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1e3a8a")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#cbd5e1")),
        ("BACKGROUND", (0, 1), (-1, 1), colors.HexColor("#ffffff")),
        ("BACKGROUND", (0, 2), (-1, 2), colors.HexColor("#f8fafc")),
        ("BACKGROUND", (0, 3), (-1, 3), colors.HexColor("#dcfce7")),  # light green for winner
    ]))
    story.append(t_shootout)
    story.append(Spacer(1, 4))
    story.append(Paragraph(
        "Parquet saved <b>84% of disk space</b> and was read <b>14 times faster</b> than JSON because it stores data vertically in columns rather than horizontally in rows.",
        body_style
    ))

    # ClickHouse
    story.append(Paragraph("D. ClickHouse (The Super-Fast Analytical Database)", h2_style))
    story.append(Paragraph(
        "<b>What it is:</b> Standard databases like MySQL or SQLite are designed to quickly look up one user or save one shopping cart item. But if you ask MySQL: <i>'Calculate the 95th percentile latency across 100,000 records grouped by country,'</i> it has to read every single row one-by-one from the hard drive, which can take seconds or minutes. ClickHouse is built specifically for aggregations.",
        body_style
    ))
    story.append(Paragraph(
        "<b>Why we added it:</b> ClickHouse inserted <b>100,000 rows in 3.67 seconds</b> (over 27,000 rows per second) and answered complex statistical queries across all 100,000 measurements in just <b>50 to 71 milliseconds</b>. This allows our dashboards to load instantly.",
        body_style
    ))

    story.append(Spacer(1, 10))

    # Section 4: Brains & Intelligence
    story.append(Paragraph("4. The Brains: Anomaly Detection & Incident Management", h1_style))

    # Anomaly Engine
    story.append(Paragraph("A. Statistical Anomaly Detection (Explainable Math, No Black-Box AI)", h2_style))
    story.append(Paragraph(
        "<b>What it is:</b> A lot of modern software claims to use 'Artificial Intelligence' to spot problems, but when an alert wakes you up at 3:00 AM, nobody knows why the AI fired. NetScope uses three transparent, explainable math checks:",
        body_style
    ))
    story.append(Paragraph("1. <b>Static Hard Cap:</b> Is the website slower than 300 milliseconds? (Simple common sense limit).", bullet_style))
    story.append(Paragraph("2. <b>Rolling Baseline Ratio:</b> What was the average speed over the past 10 minutes? Is the current speed 2x or 3x higher than normal? (Spots gradual degradation).", bullet_style))
    story.append(Paragraph("3. <b>Standard Deviation / Z-Score:</b> In basic statistics, a Z-score measures how unusual a number is compared to normal variations. If website latency is normally 20ms with a tiny variation of 2ms, a sudden spike to 650ms produces a Z-score of +40! In a standard bell curve, anything over +2.5 standard deviations happens less than 1% of the time by chance.", bullet_style))
    story.append(Paragraph(
        "<b>Why we added it:</b> Combining all three into a single score (0 to 3) ensures we catch real problems while completely ignoring harmless random spikes.",
        body_style
    ))

    # Incident Manager
    story.append(Paragraph("B. Incident Lifecycle Manager (Stop Alert Flapping)", h2_style))
    story.append(Paragraph(
        "<b>What it is:</b> Have you ever had a car alarm that chirps on and off every time a bus drives by? That is called 'alert flapping'. If a website has a 1-second hiccup, you don't want your phone buzzing with 'OUTAGE DETECTED!' followed immediately by 'RESOLVED!'.",
        body_style
    ))
    story.append(Paragraph(
        "<b>Why we added it:</b> Our Incident Manager is a strict 4-step state machine:",
        body_style
    ))
    story.append(Paragraph("• <code>NORMAL</code>: Everything is working smoothly.", bullet_style))
    story.append(Paragraph("• <code>ACTIVE</code>: The system requires <b>3 bad readings in a row</b> before it officially declares an outage.", bullet_style))
    story.append(Paragraph("• <code>RECOVERING</code>: When the website returns to normal, the alert does not close immediately; it enters a probation period.", bullet_style))
    story.append(Paragraph("• <code>RESOLVED</code>: Only after <b>2 consecutive healthy sweeps</b> is the incident marked officially closed and archived.", bullet_style))

    # Controlled Chaos
    story.append(Paragraph("C. Controlled Chaos Server (The Smoke Detector Test Button)", h2_style))
    story.append(Paragraph(
        "<b>What it is:</b> How do you test whether a fire alarm works without setting fire to your office? You press the 'test' button. We built our own small web server (running on port 8088) that has built-in test buttons:",
        body_style
    ))
    story.append(Paragraph("• <code>POST /inject/delay</code>: Makes the server artificially slow down by +650 milliseconds.", bullet_style))
    story.append(Paragraph("• <code>POST /inject/error</code>: Makes the server return HTTP 500 error codes.", bullet_style))
    story.append(Paragraph("• <code>POST /reset</code>: Instantly removes all faults and returns the server to normal sub-2ms speed.", bullet_style))
    story.append(Paragraph(
        "<b>Why we added it:</b> This allows us to prove in front of any teacher, interviewer, or evaluator that our anomaly detector, incident alerts, and self-healing logic work 100% reliably in real life.",
        body_style
    ))

    # Geographic Correlation & Traceroute
    story.append(Paragraph("D. Geographic Correlation & Traceroutes (Local vs Global Outage)", h2_style))
    story.append(Paragraph(
        "<b>What it is:</b> If your India probe sees that <code>google.com</code> is timing out, but your probes in Singapore and Europe see that <code>google.com</code> is loading in 20 milliseconds, Google is NOT down! The problem is isolated to an Indian telecom provider or an undersea fiber-optic cable cut.",
        body_style
    ))
    story.append(Paragraph(
        "<b>Why we added it:</b> Our Geographic Correlation engine checks all probes simultaneously. If only 1 out of 3 regions is failing, it reports <b>'LIKELY REGIONAL DEGRADATION'</b>. If all regions are failing simultaneously, it reports <b>'LIKELY GLOBAL OUTAGE'</b>. Furthermore, our Traceroute engine maps every router along the path and calculates an MD5 fingerprint (hash). If traffic gets rerouted around a broken cable, the route hash changes immediately, revealing the physical network shift.",
        body_style
    ))

    story.append(Spacer(1, 10))

    # Section 5: Observability & Self-Monitoring
    story.append(Paragraph("5. Platform Observability: Prometheus & Grafana", h1_style))
    story.append(Paragraph(
        "<b>What are they?</b> If NetScope is monitoring other websites, what monitors NetScope? If our Kafka consumer crashes or our probes freeze, how would anyone know? That is why we added self-monitoring:",
        body_style
    ))
    story.append(Paragraph("• <b>Prometheus:</b> A time-series recorder that visits our internal metrics exporter every 5 seconds. It counts how many total measurements were taken, checks if probe heartbeats are still ticking, and checks whether Kafka has any unread backlog (lag).", bullet_style))
    story.append(Paragraph("• <b>Grafana:</b> A visual web dashboard (on <code>http://localhost:3000</code>) with dark-mode charts, gauges, and color-coded status dials that display website latencies, probe health, and open incidents in real time.", bullet_style))

    story.append(Spacer(1, 10))

    # Section 6: Master Presentation & Summary Table
    story.append(Paragraph("6. The Master 18-Step Presentation (`final_presentation.py`)", h1_style))
    story.append(Paragraph(
        "To tie all 9 weeks of work together, we created a single master script: <code>scripts/final_presentation.py</code>. When you run this script, it steps through all 18 blueprint requirements in sequence: checking the probes, verifying Kafka speed, computing Flink windows, injecting live chaos, triggering the anomaly detector, opening an incident, diagnosing regional scope, removing the chaos, verifying self-healing, demonstrating Kafka lag recovery, executing live ClickHouse queries, and showing the storage shootout table.",
        body_style
    ))

    story.append(Spacer(1, 8))
    story.append(Paragraph("Quick Reference: Component vs Purpose", h2_style))

    summary_rows = [
        ["System Component", "Plain English Analogy", "What It Does", "Why We Added It"],
        ["Probes (Python)", "The Watchers", "Visits websites every few seconds and times connection steps", "To detect slowness from multiple cities (India, Singapore, Europe)"],
        ["Docker Compose", "Shipping Container", "Packages software so it runs cleanly on any computer", "To start ClickHouse, Kafka, MinIO, Prometheus with one single command"],
        ["Apache Kafka", "Unbreakable Conveyor Belt", "High-speed message queue that buffers measurements", "Prevents database crashes and guarantees zero lost data under heavy spikes"],
        ["Apache Flink", "Live Calculator", "Calculates rolling averages & 95th percentiles in real time", "Computes immediate statistics and discards duplicate/late network packets"],
        ["MinIO & Parquet", "Compressed Filing Cabinet", "Stores raw measurements in Hive folders using Parquet", "Cuts storage footprint by 84% (6.3x smaller) and speeds up reading by 14x"],
        ["ClickHouse", "Lightning-Fast Database", "Columnar database for fast analytical queries", "Answers complex queries across 100,000 rows in 50 to 71 milliseconds"],
        ["Anomaly Engine", "Explainable Math Detector", "Applies static limits, rolling averages, and Z-scores", "Catches real slowdowns without black-box machine learning false alarms"],
        ["Incident Manager", "Smart Alert State Machine", "Requires 3 bad rounds to open and 2 good rounds to resolve", "Stops alert flapping so engineers aren't bombarded with false alarms"],
        ["Chaos Endpoint", "Fire Alarm Test Button", "Web server where we can inject +650ms delay with one click", "Proves that our detection, alerting, and self-healing work in real life"],
        ["Geo Correlation", "Global vs Local Detective", "Compares failure rates across India, Singapore, and Europe", "Tells you if the entire website is down or just a local ISP cable"],
        ["Prometheus & Grafana", "Dashboard & Recorder", "Monitors the health of NetScope itself", "Visualizes system speed, probe heartbeats, and consumer lag at a glance"],
    ]

    t_summary = Table(summary_rows, colWidths=[90, 95, 160, 159])
    t_summary.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#0f172a")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 7.5),
        ("LEADING", (0, 0), (-1, -1), 9.5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#cbd5e1")),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.HexColor("#ffffff"), colors.HexColor("#f8fafc")]),
    ]))
    story.append(t_summary)

    story.append(Spacer(1, 14))
    story.append(create_callout(
        "<b>Summary for Interviews & Presentations:</b> NetScope is not a toy dashboard or a generic clone. It is a complete, fault-tolerant telemetry pipeline built on real-world data engineering principles: decoupled ingestion, streaming percentiles, columnar storage, explainable statistics, and automated self-healing.",
        title="Key Takeaway",
        bg_color="#eff6ff",
        border_color="#3b82f6"
    ))

    doc.build(story, canvasmaker=NumberedCanvas)
    print(f"✅ Generated PDF successfully: {output_path}")


if __name__ == "__main__":
    out_file = os.path.join(os.path.dirname(__file__), "..", "NETSCOPE_PLAIN_ENGLISH_EXPLANATION.pdf")
    generate_pdf(out_file)
