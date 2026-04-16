"""
dashboard/app.py

Streamlit dashboard for the AI Workflow Bottleneck Analyzer.
Upload a workflow CSV, pick a page from the sidebar, and explore.

Run from the project root:
    streamlit run dashboard/app.py
"""

import sys
import os
import tempfile

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# make src/ importable regardless of where streamlit is launched from
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.data_loader import load_workflow_data          # noqa: E402
from src.preprocessing import (
    preprocess_workflow,
    compute_case_durations,
    detect_exception_flows,
)  # noqa: E402
from src.bottleneck_detector import (
    detect_bottlenecks,
    generate_bottleneck_insight,
    generate_recommendation,
    generate_priority_actions,
)  # noqa: E402
from src.risk_predictor import predict_bottleneck_risk  # noqa: E402
from src.health_analyzer import calculate_workflow_health  # noqa: E402
from src.context_analyzer import detect_workflow_context  # noqa: E402
from src.automation_engine import identify_automation_opportunities  # noqa: E402
from src.insight_engine import generate_smart_insight  # noqa: E402
from src.variant_miner import (  # noqa: E402
    mine_variants,
    tag_variant_type,
    score_variants,
    generate_variant_recommendations,
    generate_variant_insights,
)

st.set_page_config(
    page_title="AI Workflow Bottleneck Analyzer",
    page_icon="🔍",
    layout="wide",
)

st.markdown(
    """
    <style>
    /* Metric cards */
    div[data-testid="stMetric"] {
        background: linear-gradient(135deg, #1e1e2f 0%, #2d2d44 100%);
        border: 1px solid #3a3a5c;
        border-radius: 12px;
        padding: 16px 20px;
    }
    /* Bottleneck alert */
    .bottleneck-alert {
        background: linear-gradient(135deg, #ff4b4b22 0%, #ff4b4b11 100%);
        border-left: 4px solid #ff4b4b;
        border-radius: 8px;
        padding: 20px 24px;
        margin: 16px 0;
    }
    .bottleneck-alert h2 {
        margin: 0 0 8px 0;
        color: #ff4b4b;
    }
    .bottleneck-alert p {
        margin: 0;
        font-size: 1.05rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ── Header ──────────────────────────────────────────────────────────────────
st.title("🔍 AI Workflow Bottleneck Analyzer")
st.markdown(
    "Upload a workflow event-log CSV to automatically detect "
    "which step is slowing down your process."
)

# ── Sidebar — file upload and page navigation ────────────────────────────────
with st.sidebar:
    st.header("📂 Data Source")
    uploaded_file = st.file_uploader(
        "Choose a workflow CSV file",
        type=["csv"],
        help="The CSV must contain: case_id, task, start_time, end_time, user",
    )

    if uploaded_file:
        st.divider()
        st.header("🧭 Navigation")
        page = st.radio(
            "Select View:",
            ["Overview", "Bottleneck Analysis", "Exception Analysis", "Risk & Insights", "Process Variants"],
            index=0,
        )

    st.divider()
    st.markdown(
        "**Expected columns:**\n"
        "- `case_id` — unique workflow instance\n"
        "- `task` — step name\n"
        "- `start_time` — when the step started\n"
        "- `end_time` — when the step finished\n"
        "- `user` — who performed the step"
    )

# ── Main analysis — only runs after file upload ──────────────────────────────
if uploaded_file is not None:

    # flag if this looks like demo/sample data
    sample_filenames = ["workflow_logs.csv", "broken_workflow_logs.csv", "data.csv"]
    if uploaded_file.name in sample_filenames:
        st.info("⚠️ **Demo Mode**: Using simulated workflow data for demonstration purposes.", icon="💡")

    # save to temp file so load_workflow_data can validate it by path
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
            tmp.write(uploaded_file.getvalue())
            tmp_path = tmp.name

        df_raw = load_workflow_data(tmp_path)
    except (FileNotFoundError, ValueError) as exc:
        st.error(f"❌ **Data loading error:** {exc}")
        st.stop()
    finally:
        # clean up temp file whether or not loading succeeded
        if "tmp_path" in locals() and os.path.exists(tmp_path):
            os.unlink(tmp_path)

    # preprocess and run all analyses
    df = preprocess_workflow(df_raw)
    case_durations = compute_case_durations(df)
    exceptions_df, total_exception_cases = detect_exception_flows(df)

    task_stats, bottleneck_task, total_bn_time, bottleneck_pct = detect_bottlenecks(df)

    # ── Page routing ─────────────────────────────────────────────────────────

    if page == "Overview":
        st.markdown("---")
        st.header("📊 Workflow Health Score")

        health_data = calculate_workflow_health(df, bottleneck_pct, total_exception_cases)

        h_score = health_data["score"]
        h_status = health_data["status"]
        h_interp = health_data["interpretation"]

        if h_score >= 80:
            st.success(f"**Current Status:** {h_status}", icon="✅")
        elif h_score >= 50:
            st.warning(f"**Current Status:** {h_status}", icon="⚠️")
        else:
            st.error(f"**Current Status:** {h_status}", icon="🔴")

        hcol1, hcol2 = st.columns([1, 4])
        hcol1.metric("Health Score", f"{h_score}/100")
        with hcol2:
            st.markdown(f"**AI Interpretation:** {h_interp}")
            if health_data.get("top_contributors"):
                st.markdown("**🔍 Key Contributors to Low Score:**")
                for contributor in health_data["top_contributors"]:
                    st.markdown(f"* {contributor}")

        st.markdown("---")
        st.header("🧠 Workflow Context")
        context = detect_workflow_context(df)

        ctx_col1, ctx_col2 = st.columns(2)
        ctx_col1.info(f"**Workflow Type:** {context['workflow_type']}", icon="🏷️")

        with ctx_col2:
            st.markdown("**Process Breakdown:**")
            if context["human_tasks"]:
                st.markdown(f"👤 **Human Tasks:** {', '.join(context['human_tasks'])}")
            if context["system_tasks"]:
                st.markdown(f"⚙️ **System Tasks:** {', '.join(context['system_tasks'])}")

        st.markdown("---")
        st.header("📋 Workflow Overview")
        mcol1, mcol2, mcol3, mcol4 = st.columns(4)

        mcol1.metric("📊 Total Events", f"{len(df):,}")
        mcol2.metric("📁 Unique Cases", f"{df['case_id'].nunique():,}")
        mcol3.metric("📋 Workflow Steps", f"{df['task'].nunique()}")
        mcol4.metric("⚠ Top Bottleneck", bottleneck_task)

        st.markdown("---")
        st.header("📉 Duration Distribution")
        st.markdown("Shows the distribution of total end-to-end time for all cases.")

        fig_hist = px.histogram(
            case_durations,
            x="total_duration_minutes",
            nbins=20,
            title="<b>Case Completion Time Distribution</b>",
            labels={"total_duration_minutes": "Total Duration (minutes)"},
            color_discrete_sequence=["#636efa"],
        )
        fig_hist.update_layout(
            xaxis_title="End-to-End Duration (minutes)",
            yaxis_title="Number of Cases",
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            height=350,
            margin=dict(t=50, b=40, l=40, r=40),
        )
        st.plotly_chart(fig_hist, width="stretch")

        st.markdown("---")
        st.header("⏱ SLA Compliance Analysis")
        st.markdown("Monitors task execution against defined Service Level Agreements (SLA).")

        # thresholds match preprocessing.py — keep in sync if you change them there
        SLA_MAP = {
            "Lead Created": 5, "Lead Reviewed": 30, "Manager Approval": 120,
            "Proposal Sent": 60, "Deal Closed": 60
        }

        sla_data = df.groupby("task").agg(
            avg_dur=("duration_minutes", "mean"),
            violations=("sla_violation", "sum"),
            total=("sla_violation", "count")
        ).reset_index()
        sla_data["violation_pct"] = (sla_data["violations"] / sla_data["total"]) * 100
        sla_data["target_sla"] = sla_data["task"].map(SLA_MAP)
        sla_data = sla_data.sort_values("violation_pct", ascending=False)

        scol1, scol2 = st.columns([2, 3])

        with scol1:
            st.subheader("📋 Compliance Summary")
            table_display = sla_data[["task", "target_sla", "avg_dur", "violation_pct"]].copy()
            table_display.columns = ["Task", "SLA Target (min)", "Avg Duration", "Violation %"]
            st.dataframe(
                table_display.style.format({
                    "Avg Duration": "{:,.1f}m",
                    "Violation %": "{:.1f}%",
                    "SLA Target (min)": "{:,.0f}m"
                }).highlight_between(left=80, right=100, subset=["Violation %"], color="#ff4b4b33"),
                width="stretch", hide_index=True
            )

            top_violator = sla_data.iloc[0]
            if top_violator["violation_pct"] > 50:
                st.warning(
                    f"**{top_violator['task']}** exceeds SLA in **{top_violator['violation_pct']:.1f}%** of cases, "
                    "indicating a critical performance issue requiring attention.",
                    icon="⏱️"
                )
            else:
                st.success("SLA compliance is within acceptable parameters.", icon="✅")

        with scol2:
            st.subheader("📊 Violation Rate by Step")
            fig_sla = px.bar(
                sla_data, x="task", y="violation_pct",
                title="<b>SLA Violation % per Workflow Step</b>",
                labels={"violation_pct": "Violation Rate (%)", "task": "Workflow Step"},
                color_discrete_sequence=["#e74c3c"]
            )
            fig_sla.update_layout(plot_bgcolor="rgba(0,0,0,0)", height=350)
            st.plotly_chart(fig_sla, width="stretch")

        st.divider()
        with st.expander("🔎 Preview Raw Data", expanded=False):
            st.dataframe(df.head(50), width="stretch", hide_index=True)

    elif page == "Bottleneck Analysis":
        st.markdown("---")
        st.header("⚠ Bottleneck Analysis")
        avg_val = task_stats.loc[bottleneck_task, "avg_duration_minutes"]
        median_val = task_stats.loc[bottleneck_task, "median_duration_minutes"]

        if df["task"].nunique() > 1 and avg_val > 0:
            st.markdown(
                f"""
                <div class="bottleneck-alert">
                    <h2>⚠️ {bottleneck_task}</h2>
                    <p>
                        This step has the <strong>highest average duration</strong>
                        across all workflow cases.<br>
                        Average: <strong>{avg_val:,.2f} min</strong> &nbsp;|&nbsp;
                        Median: <strong>{median_val:,.2f} min</strong>
                    </p>
                </div>
                """,
                unsafe_allow_html=True,
            )
        else:
            st.success("No significant bottleneck detected.", icon="✅")

        st.markdown("---")
        st.header("🎯 Bottleneck Impact Analysis")
        st.markdown(
            "Understand how much of your team's total workflow time is being "
            "consumed by the bottleneck step."
        )

        total_workflow_time = df["duration_minutes"].sum()
        other_time = total_workflow_time - total_bn_time
        bn_case_count = int(task_stats.loc[bottleneck_task, "count"])

        ic1, ic2, ic3 = st.columns(3)
        ic1.metric(
            label="⏳ Total Time in Bottleneck",
            value=f"{total_bn_time:,.0f} min",
            help="Sum of duration_minutes across every execution of the bottleneck task.",
        )
        ic2.metric(
            label="📉 Share of Total Workflow Time",
            value=f"{bottleneck_pct:.1f}%",
            help="Bottleneck total time ÷ grand total of all step durations × 100.",
        )
        ic3.metric(
            label="🗂️ Cases Affected",
            value=f"{bn_case_count:,}",
            help="Number of workflow cases that passed through the bottleneck step.",
        )

        donut_labels = [f"🔴 {bottleneck_task}", "🔵 All Other Steps"]
        donut_values = [total_bn_time, max(other_time, 0)]
        donut_colors = ["#ff4b4b", "#636efa"]

        fig_donut = go.Figure(
            go.Pie(
                labels=donut_labels,
                values=donut_values,
                hole=0.55,
                marker=dict(colors=donut_colors, line=dict(color="#111", width=2)),
                textinfo="label+percent",
                hovertemplate="%{label}<br>%{value:,.0f} min (%{percent})<extra></extra>",
            )
        )
        fig_donut.update_layout(
            showlegend=False,
            height=340,
            margin=dict(t=20, b=10, l=10, r=10),
            annotations=[
                dict(
                    text=f"<b>{bottleneck_pct:.1f}%</b><br>bottleneck",
                    x=0.5,
                    y=0.5,
                    font=dict(size=16, color="#ff4b4b"),
                    showarrow=False,
                )
            ],
        )
        st.plotly_chart(fig_donut, width="stretch")

        st.markdown("---")
        st.header("📈 Business Impact")
        st.markdown("Translating technical delays into potential operational gains.")

        # using 25% as a standard optimization target — conservative but realistic
        target_reduction = 0.25
        savings_per_case = avg_val * target_reduction
        total_potential_savings = total_bn_time * target_reduction
        workflow_impact_pct = (total_potential_savings / total_workflow_time) * 100

        bicol1, bicol2 = st.columns(2)
        bicol1.info(
            f"**Potential Efficiency Gain**\n\n"
            f"Optimizing this step could reduce total workflow time by **~{workflow_impact_pct:.1f}%**.",
            icon="📈"
        )
        bicol2.success(
            f"**Operational Time Savings**\n\n"
            f"Estimated **{total_potential_savings:,.0f} minutes** saved across all workflows (**{savings_per_case:.1f}m** per case).",
            icon="💡"
        )

        st.markdown("---")
        st.header("🧪 What-if Scenario Analysis")
        st.markdown(
            "Simulate the impact of optimizing the bottleneck. If you reduce the "
            f"duration of **{bottleneck_task}**, how much faster would the entire "
            "workflow become?"
        )

        num_executions = int(task_stats.loc[bottleneck_task, "count"])
        rec = generate_recommendation(bottleneck_task, avg_val, num_executions)
        try:
            default_reduction = int(rec['estimated_savings'].split("–")[0].strip().replace("%", ""))
        except (ValueError, IndexError):
            default_reduction = 20  # fallback if parsing fails

        reduction_pct = st.slider(
            f"Optimize '{bottleneck_task}' by %",
            min_value=0,
            max_value=100,
            value=min(default_reduction, 100),
            step=5,
        )

        current_avg_completion = case_durations["total_duration_minutes"].mean()
        time_saved = avg_val * (reduction_pct / 100)
        simulated_avg_completion = max(0, current_avg_completion - time_saved)
        overall_improvement = (time_saved / current_avg_completion * 100) if current_avg_completion > 0 else 0

        sc1, sc2, sc3 = st.columns(3)
        sc1.metric("Current Avg Completion", f"{current_avg_completion:.1f} min")
        sc2.metric("Simulated Avg Completion", f"{simulated_avg_completion:.1f} min", delta=f"-{time_saved:.1f} min")
        sc3.metric("Overall Improvement", f"{overall_improvement:.1f}%", delta=f"{reduction_pct}% local fix", delta_color="off")

        st.markdown("---")
        st.header("⏳ Waiting vs Processing Time")
        st.markdown(
            "Understand whether delays are due to active work (Processing) "
            "or idle queues (Waiting)."
        )

        wp_stats = df.groupby("task")[["processing_time_minutes", "waiting_time_minutes"]].mean().reset_index()
        wp_stats["total"] = wp_stats["processing_time_minutes"] + wp_stats["waiting_time_minutes"]
        wp_stats = wp_stats.sort_values("total", ascending=False)

        wp_melted = wp_stats.melt(
            id_vars=["task"],
            value_vars=["processing_time_minutes", "waiting_time_minutes"],
            var_name="Time Type",
            value_name="Minutes"
        )
        wp_melted["Time Type"] = wp_melted["Time Type"].map({
            "processing_time_minutes": "Processing Time",
            "waiting_time_minutes": "Waiting Time"
        })

        fig_wp = px.bar(
            wp_melted,
            x="task",
            y="Minutes",
            color="Time Type",
            barmode="stack",
            title="<b>Average Processing vs Waiting Delay per Task</b>",
            labels={"task": "Workflow Step", "Minutes": "Avg Duration (min)"},
            color_discrete_map={"Processing Time": "#2ecc71", "Waiting Time": "#e67e22"}
        )
        fig_wp.update_layout(plot_bgcolor="rgba(0,0,0,0)", height=400)
        st.plotly_chart(fig_wp, width="stretch")

        bn_wp = wp_stats.loc[wp_stats["task"] == bottleneck_task].iloc[0]
        wait_pct = (bn_wp["waiting_time_minutes"] / bn_wp["total"]) * 100

        st.info(
            f"💡 **Delay Insight:** Majority ({wait_pct:.1f}%) of delay in **{bottleneck_task}** "
            f"is due to **waiting time**, indicating idle queue delays or approval backlog.",
            icon="⏳"
        )

        st.markdown("---")
        st.header("📊 Task Distribution & Sequencing")
        tab1, tab2, tab3, tab4 = st.tabs(["Timeline", "Stats Table", "Bar Chart", "Box Plot"])

        with tab1:
            st.subheader("⏱️ Workflow Timeline Overview")
            WORKFLOW_ORDER = ["Lead Created", "Lead Reviewed", "Manager Approval", "Proposal Sent", "Deal Closed"]
            known_tasks = [t for t in WORKFLOW_ORDER if t in task_stats.index]
            unknown_tasks = sorted(set(task_stats.index) - set(WORKFLOW_ORDER))
            ordered_tasks = known_tasks + unknown_tasks
            timeline_df = task_stats.loc[ordered_tasks].reset_index()
            timeline_df.columns = ["Task", "Avg Duration (min)", "Median Duration (min)", "Execution Count"]
            timeline_df["Cumulative (min)"] = timeline_df["Avg Duration (min)"].cumsum()
            bar_colors = ["#ff4b4b" if task == bottleneck_task else "#636efa" for task in timeline_df["Task"]]

            fig_timeline = go.Figure()
            fig_timeline.add_trace(go.Bar(
                y=timeline_df["Task"], x=timeline_df["Avg Duration (min)"], orientation="h",
                marker_color=bar_colors, text=[f"{d:,.1f} min" for d in timeline_df["Avg Duration (min)"]], textposition="auto"
            ))
            for _, row in timeline_df.iterrows():
                fig_timeline.add_annotation(x=row["Avg Duration (min)"], y=row["Task"], text=f"  Σ {row['Cumulative (min)']:,.0f} min", showarrow=False, xanchor="left", font=dict(size=11, color="#888"))
            fig_timeline.update_layout(
                title="<b>Workflow Execution Sequence & Latency</b>",
                yaxis=dict(autorange="reversed"),
                plot_bgcolor="rgba(0,0,0,0)",
                height=400,
                margin=dict(l=20, r=80, t=50, b=40)
            )
            st.plotly_chart(fig_timeline, width="stretch")

        with tab2:
            st.subheader("📋 Task Duration Statistics")
            stats_display = task_stats.reset_index()
            stats_display.columns = ["Task", "Avg Duration (min)", "Median Duration (min)", "Execution Count"]
            st.dataframe(stats_display.style.format({"Avg Duration (min)": "{:,.2f}", "Median Duration (min)": "{:,.2f}", "Execution Count": "{:,}"}).highlight_max(subset=["Avg Duration (min)"], color="#ff4b4b33"), width="stretch", hide_index=True)

        with tab3:
            st.subheader("📊 Average Duration per Task")
            timeline_df["Color"] = timeline_df["Task"].apply(
                lambda t: "#ff4b4b" if t == bottleneck_task else "#636efa"
            )
            fig_bar = px.bar(
                timeline_df,
                x="Task",
                y="Avg Duration (min)",
                text="Avg Duration (min)",
                title="<b>Mean Execution Time by Step</b>",
                labels={"Avg Duration (min)": "Minutes", "Task": "Workflow Step"},
                color="Color",
                color_discrete_map="identity"
            )
            fig_bar.update_traces(texttemplate='%{text:.1f}m', textposition='outside')
            fig_bar.update_layout(plot_bgcolor="rgba(0,0,0,0)", height=400, showlegend=False)
            st.plotly_chart(fig_bar, width="stretch")

        with tab4:
            st.subheader("📦 Task Duration Distribution")
            fig_box = px.box(
                df,
                x="task",
                y="duration_minutes",
                color="task",
                points="outliers",
                title="<b>Variance and Outlier Detection</b>",
                labels={"duration_minutes": "Duration (min)", "task": "Step Name"}
            )
            fig_box.update_layout(plot_bgcolor="rgba(0,0,0,0)", height=400, showlegend=False)
            st.plotly_chart(fig_box, width="stretch")

    elif page == "Exception Analysis":
        st.markdown("---")
        st.header("⚠ Workflow Exceptions")
        st.markdown("Identifies cases with process violations like rework loops or unexpected task sequences.")

        total_unique_cases = df["case_id"].nunique()
        exception_pct = (total_exception_cases / total_unique_cases) * 100 if total_unique_cases > 0 else 0

        if total_exception_cases > 0:
            st.warning(
                f"**{total_exception_cases}** exception cases detected (**{exception_pct:.1f}%** of all workflows).\n\n"
                "Repeated task patterns detected. This indicates rework and process inefficiency.",
                icon="⚠️"
            )

            st.dataframe(
                exceptions_df,
                column_config={"case_id": "Case ID", "issue_type": "Issue Type", "affected_task": "Affected Task"},
                use_container_width=True, hide_index=True,
            )
        else:
            st.success("No major workflow exceptions detected. Process is stable.", icon="✅")

        st.subheader("💡 Exception Insights")
        if total_exception_cases == 0:
            st.info("**Process is Healthy.**\n\nNo task loops or sequence deviations detected.", icon="✅")
        elif exception_pct < 5:
            st.info(f"**Low Exception Rate Detected ({exception_pct:.1f}%).**\n\nThe process is mostly stable.", icon="💡")
        else:
            st.warning(f"**High Exception Rate Detected ({exception_pct:.1f}%).**\n\nProcess inefficiency is significant.", icon="⚠️")

    elif page == "Risk & Insights":
        st.markdown("---")
        
        # ── Pre-calculate Decisions ───────────────────────────────────────────
        avg_val = task_stats.loc[bottleneck_task, "avg_duration_minutes"]
        num_executions = int(task_stats.loc[bottleneck_task, "count"])
        
        wp_stats = df.groupby("task")[["processing_time_minutes", "waiting_time_minutes"]].mean()
        bn_wait = wp_stats.loc[bottleneck_task, "waiting_time_minutes"]
        bn_total = wp_stats.loc[bottleneck_task].sum()
        wait_pct = (bn_wait / bn_total) * 100
        
        rec_data = generate_recommendation(bottleneck_task, avg_val, num_executions)
        smart_insight = generate_smart_insight(bottleneck_task, avg_val, wait_pct, rec_data)
        
        risk_results = predict_bottleneck_risk(df, task_stats)
        task_risks = next((r for r in risk_results if r["task"] == bottleneck_task), None)

        # 🚨 Primary Bottleneck — The core problem
        st.markdown("### 🚨 Primary Bottleneck")
        st.error(
            f"**Task:** {bottleneck_task}  \n"
            f"**Avg Duration:** {avg_val:,.1f}m  |  "
            f"**Frequency:** {num_executions} cases\n\n"
            f"{smart_insight['cause']}"
        )

        # 🎯 Recommended Action — The solution
        st.markdown("### 🎯 Recommended Action")
        st.success(f"**{smart_insight['action']}**")

        # 📊 Expected Impact — The result
        st.markdown("### 📊 Expected Impact")
        st.info(f"**{smart_insight['impact']}**")

        # ⚠️ Supporting Risks — Secondary signals
        if task_risks and task_risks.get("factors"):
            st.markdown("### ⚠️ Supporting Risks")
            display_factors = task_risks["factors"][:2]
            if display_factors:
                st.warning("\n".join([f"* {f}" for f in display_factors]))

        st.markdown("---")

        # ⚙️ Automation Opportunities — Extra technical improvements
        st.markdown("### ⚙️ Automation Opportunities")
        automation_ops = identify_automation_opportunities(df, task_stats)
        
        # Filter out the primary bottleneck to avoid repetition
        new_ops = [op for op in automation_ops if op['task'] != bottleneck_task]
        
        if new_ops:
            for op in new_ops[:2]:
                with st.expander(f"System-level Suggestion: {op['task']}", expanded=True):
                    st.markdown(f"**Technical Root:** {op['reason']}")
                    st.markdown(f"**Integration Path:** {op['suggestion']}")
                    if op.get("impact_text"):
                        st.markdown(f"**Impact:** {op['impact_text']}")
        else:
            st.info("Process is largely optimized. No additional automation opportunities identified outside the primary bottleneck.", icon="⚙️")

    elif page == "Process Variants":
        st.markdown("---")
        st.header("🔀 Process Variants")
        st.markdown(
            "Discover how many different paths your workflows actually take, "
            "and which deviations cost the most time."
        )

        # ── STEP 1 — SLA control ─────────────────────────────────────────
        sla_hours = st.slider(
            "SLA threshold (hours)",
            min_value=4,
            max_value=72,
            value=24,
            step=1,
        )

        # ── STEP 2 — Run the pipeline ────────────────────────────────────
        variant_df = mine_variants(df)

        if variant_df.empty:
            st.warning("No variants could be mined from this dataset.", icon="⚠️")
        else:
            standard_path = variant_df.iloc[0]["fingerprint"].split("|")
            variant_df["variant_type"] = variant_df["fingerprint"].apply(
                lambda fp: tag_variant_type(fp, standard_path)
            )
            variant_df = score_variants(df, variant_df, sla_limit_hours=sla_hours)
            variant_df = generate_variant_recommendations(variant_df)

            # ── STEP 3 — KPI cards ───────────────────────────────────────
            col1, col2, col3, col4 = st.columns(4)
            total_variants = len(variant_df)
            conformant_count = int(
                (variant_df["variant_type"] == "Conformant").sum()
            )
            rework_count = int(
                (variant_df["variant_type"] == "Rework loop").sum()
            )
            top_variant_pct = round(
                variant_df.iloc[0]["frequency"]
                / variant_df["frequency"].sum()
                * 100,
                1,
            )

            col1.metric("Total Variants", total_variants)
            col2.metric("Conformant Paths", conformant_count)
            col3.metric("Rework Loops", rework_count)
            col4.metric("Top Variant Share", f"{top_variant_pct}%")

            # ── STEP 4 — Display DataFrame ───────────────────────────────
            st.markdown("---")
            st.subheader("📋 Variant Details")

            display_cols = [
                "variant_label",
                "frequency",
                "avg_cycle_hours",
                "sla_breach_rate",
                "avg_wait_ratio",
                "variant_type",
                "drift_label",
                "recommendation",
            ]
            display_df = variant_df[display_cols].copy()

            # format for display
            display_df["sla_breach_rate"] = display_df["sla_breach_rate"].astype(float)
            display_df["avg_wait_ratio"] = display_df["avg_wait_ratio"].astype(float)
            display_df["avg_cycle_hours"] = display_df["avg_cycle_hours"].round(1)

            # sort by sla_breach_rate descending
            display_df = display_df.sort_values(
                "sla_breach_rate", ascending=False
            ).reset_index(drop=True)

            # percentage strings for display copy
            display_df["sla_breach_rate"] = display_df["sla_breach_rate"].apply(
                lambda v: f"{v * 100:.1f}%"
            )
            display_df["avg_wait_ratio"] = display_df["avg_wait_ratio"].apply(
                lambda v: f"{v * 100:.1f}%"
            )

            # color rows by variant_type
            type_colors = {
                "Conformant": "background-color: #e6f4ea",
                "Rework loop": "background-color: #fce8e6",
                "Skip": "background-color: #fff3e0",
                "Extended": "background-color: #e8f0fe",
            }

            def _color_by_type(val):
                return type_colors.get(val, "")

            styled = display_df.style.map(
                _color_by_type, subset=["variant_type"]
            )

            st.dataframe(styled, use_container_width=True, hide_index=True)

            # ── STEP 5 — Business insights ───────────────────────────────
            st.markdown("---")
            insights = generate_variant_insights(variant_df)
            st.markdown("### Key Insights")
            for insight in insights:
                st.info(insight)

            # ── Smart Warning (with Upgrade 3) ───────────────────────────
            st.markdown("---")
            st.subheader("⚠️ Highest-Risk Deviant Path")

            non_conformant = variant_df[
                variant_df["variant_type"] != "Conformant"
            ].copy()

            if len(non_conformant) > 0:
                max_cycle = non_conformant["avg_cycle_hours"].max()
                max_sla = non_conformant["sla_breach_rate"].max()

                non_conformant["_risk_score"] = (
                    0.5 * (non_conformant["avg_cycle_hours"] / (max_cycle + 1e-9))
                    + 0.5 * (non_conformant["sla_breach_rate"] / (max_sla + 1e-9))
                )

                worst = non_conformant.loc[
                    non_conformant["_risk_score"].idxmax()
                ]

                total_cases = variant_df["frequency"].sum()
                case_pct = round(
                    worst["frequency"] / total_cases * 100, 1
                )

                # UPGRADE 3 — "What if we fix this?" saving estimate
                best_cycle = variant_df["avg_cycle_hours"].min()
                potential_saving = max(
                    0, worst["avg_cycle_hours"] - best_cycle
                )
                total_potential = round(
                    potential_saving * worst["frequency"], 0
                )

                st.warning(
                    f"**{worst['variant_label']} ({worst['variant_type']})** "
                    f"is your highest-risk deviant path, used in **{case_pct}% of cases**. "
                    f"Average cycle time: **{worst['avg_cycle_hours']:.1f} hours**. "
                    f"SLA breach rate: **{worst['sla_breach_rate'] * 100:.0f}%**. "
                    f"{worst['recommendation']} "
                    f"If optimized to match the best-performing variant, "
                    f"this could reduce cycle time by ~{potential_saving:.1f} hours per case "
                    f"— a total of ~{total_potential:.0f} hours recovered across {int(worst['frequency'])} cases."
                )

            else:
                st.success(
                    "All workflow variants are conformant. "
                    "No deviant paths detected in this dataset."
                )

            # ── Bar Chart — Avg Cycle Time by Variant ────────────────────
            st.markdown("---")
            st.subheader("📊 Cycle Time by Variant")

            chart_df = variant_df.sort_values("avg_cycle_hours", ascending=False)

            fig = px.bar(
                chart_df,
                x="variant_label",
                y="avg_cycle_hours",
                color="variant_type",
                color_discrete_map={
                    "Conformant":  "#34a853",
                    "Rework loop": "#ea4335",
                    "Skip":        "#fbbc04",
                    "Extended":    "#4285f4",
                },
                title="Average Cycle Time by Process Variant",
                labels={
                    "variant_label": "Variant",
                    "avg_cycle_hours": "Avg Cycle Time (hours)",
                    "variant_type": "Type",
                },
                text="avg_cycle_hours",
            )

            fig.update_traces(texttemplate="%{text:.1f}h", textposition="outside")
            fig.update_layout(
                uniformtext_minsize=10,
                uniformtext_mode="hide",
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                showlegend=True,
            )

            st.plotly_chart(fig, use_container_width=True)

else:
    # nothing uploaded yet — show instructions
    st.info(
        "📂 **Upload a workflow dataset to begin analysis.**\n\n"
        "👈 Use the sidebar on the left to select your CSV file.\n\n"
        "Need sample data? Run: `python src/generate_dataset.py`"
    )

# ── Footer ───────────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown(
    """
    <div style="text-align: center; color: #888; font-size: 0.85rem; padding: 20px 0;">
        <strong>AI Workflow Intelligence System</strong><br>
        Process analytics, bottleneck detection, and optimization insights
    </div>
    """,
    unsafe_allow_html=True
)
