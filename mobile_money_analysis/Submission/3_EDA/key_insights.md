# Key Insights — Mobile Money EDA

Dataset: 16 users, 7772 transactions, total volume ~140014111.4M XAF

1. **Skewed activity distribution**: Average 13.8 transactions/month per user
   but distribution is right-skewed — a few high-activity users drive most of the volume.
   5 users fall in the High tier.

2. **OUT-flow dominance**: 84.3% of all transactions are OUT (sent/spent),
   confirming that mobile money is used predominantly for spending and bill payment
   rather than receiving funds.

3. **Top transaction type — `transfert`** (53.2% of all rows). Payments
   (paiement) and transfers form the bulk of usage; deposits are far less frequent.

4. **Temporal pattern**: Activity peaks in 2025-12; the Day-x-Hour heatmap shows
   strong concentration on weekdays during business hours (08:00-19:00) with a
   secondary evening peak. Weekend share averages 26.9% per user.

5. **Income–volume link**: Higher income brackets show higher total OUT volumes,
   but the relationship is non-linear — high-frequency low-income users can match
   spend volume of mid-income users (visible in Fig 6).

6. **Strong feature correlations**: total_transactions ↔ tx_per_month (>0.9),
   n_out ↔ total_amount_out, and avg_amount ↔ std_amount, suggesting redundancy
   that should be handled (regularization or feature selection) at modeling stage.

## Hypotheses for the predictive stage
- H1: Users can be reliably classified into Low/Medium/High activity tiers using
  tx_per_month, total_amount_out and weekend_ratio alone.
- H2: Demographic features (occupation, income, zone) marginally improve classification
  but only when combined with behavioural features.
- H3: send_receive_ratio is a discriminative feature for separating "spenders" from
  "receivers".

## Predictive features candidates
tx_per_month, total_amount_out, total_amount_in, send_receive_ratio,
weekend_ratio, n_tx_types, std_amount, avg_balance.
