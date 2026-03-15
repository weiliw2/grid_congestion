# Final Report

## Question 1: Which regions show persistent price separation?
- ERCOT: median nodal price separation is 10.40, median node-to-hub spread is 3.95, and average absolute congestion is 7.28 across 2 intervals.
- PJM: median nodal price separation is 8.50, median node-to-hub spread is 2.15, and average absolute congestion is 6.45 across 2 intervals.
- ERCOT: West Export (ERCOT_B) is the top recurring congestion-exposed node with average absolute congestion of 12.90.
- PJM: Central Interface (PJM_B) is the top recurring congestion-exposed node with average absolute congestion of 11.40.

## Question 2: How much of the spread looks like congestion?
- ERCOT: average absolute congestion is 7.28 while median price separation is 10.40 and median node-to-hub spread is 3.95, suggesting spread and congestion move together in the sample.
- PJM: average absolute congestion is 6.45 while median price separation is 8.50 and median node-to-hub spread is 2.15, suggesting spread and congestion move together in the sample.
- Spread is treated here as a congestion signal candidate rather than a definitive measure, because losses and market design effects can also contribute.

## Question 3: Do high-renewable or low-net-load periods show abnormal spreads or negative prices?
- ERCOT: high-renewable intervals show 0.83 lower average absolute congestion than low-renewable intervals.
- PJM: high-renewable intervals show 2.17 lower average absolute congestion than low-renewable intervals.
- ERCOT: negative price frequency is 0.0% in the current sample.
- PJM: negative price frequency is 0.0% in the current sample.
- Stress-event intervals remain useful candidates for deeper low-net-load and renewable interaction checks:
- ERCOT 2025-07-01T00:00:00+00:00: mean absolute congestion 7.70, price separation 8.40.
- PJM 2025-07-01T00:00:00+00:00: mean absolute congestion 7.53, price separation 7.60.

## Question 4: What are the implications for transmission, storage, and modernization?
- Persistent high-spread nodes are candidate locations for transmission reinforcement or operational bottleneck review.
- If future runs show repeated spread during renewable-rich intervals, those regions become stronger candidates for storage deployment.
- Sustained nodal divergence across time would support broader grid modernization investment where renewable growth is outrunning transfer capability.
