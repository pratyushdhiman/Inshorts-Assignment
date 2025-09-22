# Inshorts Data Engineering Assignment

## Executive Summary
This project implements a **Medallion Architecture (Bronze–Silver–Gold)** pipeline on Inshorts user, event, and content data using **PySpark**.  

- **Users**: 42.5K installs, Android dominant, organic growth backbone.  
- **Content**: 113K items, English-heavy, centralized publishing.  
- **Events**: 81M+ logs, funnel skewed to Shown/Front, retention challenges.  
- **Combined (Gold)**: Organic + Android users drive engagement, retention is strong short-term but long-term churn exists, category churn highlights sticky vs fragile segments.  

---

## Users Analysis
- The app acquired **42.5k users in January 2024**, with a stable daily acquisition rate.  
- **Android is the dominant platform (82%)**, while iOS accounts for only 18%.  
- **English users dominate** the base, with limited penetration in Hindi-speaking regions.  
- **Organic installs are the backbone of growth (63%)**, while Campaign 4 is the only effective paid campaign.  
- Other campaigns contributed minimally and could be optimized or discontinued.  

---

## Events Analysis
- The platform logs **81M+ events**, dominated by “Shown” and “Front” actions.  
- **DAU grew strongly in early January**, but retention issues led to declining WAU and MAU from February onwards.  
- **Back events show deeper reading engagement (~40s avg)**, while “Front” is quick-swipe behavior.  
- **Outliers exist (>3h timespent)**, suggesting idle usage.  
- Engagement is concentrated in a **small set of power users**, highlighting reliance on heavy users rather than broad-based depth.  

---

## Content Analysis
- The content base is large (**113K items**) and has grown steadily since 2015.  
- It is **English-heavy (77%)**, with Hindi (22%) as a key secondary language.  
- Categories emphasize **National, Sports, Politics, Entertainment, and Business**, showing alignment with Indian news consumption habits.  
- A small number of **system authors generate the majority of content**, indicating a highly centralized publishing process.  

---

## Combined (Gold) Analysis
**`Funnel Performance by User Segments`**:

- Android dominates: ~31.3M Front, ~30.5M Shown events.

- iOS smaller: ~7.0M Front, ~5.1M Shown.

- NULL platform rows (~2.7M Shown, ~2.3M Front) reflect missing user metadata. 
- Most engagement is Android-driven, but some events cannot be tied back to a known platform.

**`Engagement by Content Category`**:
- National leads with ~33M interactions (avg timespent ~5s).

- Sports (12.6M) and Entertainment (12.3M) also major drivers.

- Politics, Business, World each contribute 8–10M.

- Niche categories like Hatke (715K, avg ~10s) and Miscellaneous (2.5M, avg ~7.5s) show higher per-interaction depth, despite lower scale.
- Broad categories drive volume; niche categories drive depth.

**`Campaign Effectiveness Beyond Installs`**:
- Organic users (26.7K) → ~53.1M events, avg timespent 5.6s.

- Campaign 4 (12.3K users) → ~19.7M events, avg timespent 4.5s.

- Other campaigns: lower users & events, often with short engagement (3–4s avg).

- A few outliers (Campaign 2, 12) show unusually high avg timespent, but with very few users (<15).

- Organic is the backbone of both scale and quality. Campaign 4 is the only significant paid contributor.

**`Retention (D1, W1, M1)`**:
- Day-1 retention: ~65% of users returned the next day.

- Week-1 retention: ~71% returned within the first week.

- Month-1 retention: ~74% returned within the first month.

- Retention is consistent and shows that many users keep coming back across multiple time horizons.

**`Weekly Churn Analysis`**:
- Active users grew from 10.6K (Week 1) → 27.6K (Week 5).

- After Week 5, active users declined gradually.

- Weekly churn was ~20–25% in early weeks, stabilizing later.

- By Week 18, churn = 100% (all users dropped, likely dataset cutoff).

- Engagement ramped up quickly in January but couldn’t sustain growth in later weeks.

**`Category-Level Churn Analysis`**:
- National: ~298K users, majority retained (242K), with moderate churn (~43K) and switching (~13K).

- Entertainment: ~12K users, only ~2.8K retained; most switched or churned.

- Sports: ~10.9K users, balanced split between retention (~3.4K), churn (~3.4K), switching (~4.1K).

- Smaller categories (Education, Travel, Hatke, Lifestyle) had high churn (>60%) and very low retention.

- Core news categories (National, Sports) retain users better, while peripheral categories lose users more quickly or see them switch interests.

---

## Closing Notes
- The pipeline successfully demonstrates **Bronze → Silver → Gold transformation**.  
- Insights provide a clear picture of **user acquisition, engagement, retention, and churn**.  
- Future improvements:  
  - Automate ingestion & processing with Airflow.  
  - Store outputs in Delta Lake.  
  - Add dashboards for funnel, retention, and churn visualization.  
