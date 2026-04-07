package notifications

import (
	"fmt"
	"log"
	"math"
	"strings"
	"time"
)

// CheckOverdueBills sends an email to admins if there are overdue AP invoices.
func (e *EmailSender) CheckOverdueBills() {
	today := time.Now().Format("2006-01-02")

	type overdueBill struct {
		ContactName   string   `json:"contact_name"`
		InvoiceNumber string   `json:"invoice_number"`
		DueDate       string   `json:"due_date"`
		AmountDue     *float64 `json:"amount_due"`
	}

	var bills []overdueBill
	e.GormDB.Raw(`
		SELECT contact_name, invoice_number,
			   TO_CHAR(due_date, 'YYYY-MM-DD') as due_date, amount_due
		FROM xero_invoices
		WHERE UPPER(type) = 'ACCPAY'
		  AND UPPER(status) = 'AUTHORISED'
		  AND due_date < ?
		ORDER BY due_date ASC
	`, today).Scan(&bills)

	if len(bills) == 0 {
		log.Println("Alerts: no overdue bills")
		return
	}

	totalOverdue := 0.0
	var rows []string
	for _, b := range bills {
		amt := 0.0
		if b.AmountDue != nil {
			amt = *b.AmountDue
		}
		totalOverdue += amt

		daysOverdue := 0
		if due, err := time.Parse("2006-01-02", b.DueDate); err == nil {
			daysOverdue = int(time.Since(due).Hours() / 24)
		}

		rows = append(rows, fmt.Sprintf(
			"<tr><td style='padding:6px 12px'>%s</td><td style='padding:6px 12px'>%s</td>"+
				"<td style='padding:6px 12px'>%s</td><td style='padding:6px 12px;text-align:right'>$%.2f</td>"+
				"<td style='padding:6px 12px;text-align:right'>%d days</td></tr>",
			b.ContactName, b.InvoiceNumber, b.DueDate, amt, daysOverdue))
	}

	subject := fmt.Sprintf("ESC Business Analytics — %d Overdue Bills", len(bills))
	body := fmt.Sprintf(`
	<div style="font-family:Arial,sans-serif;max-width:700px">
		<h2 style="color:#b91c1c">%d Overdue Bills — $%.2f Total</h2>
		<table style="border-collapse:collapse;width:100%%">
			<thead>
				<tr style="background:#f1f5f9">
					<th style="padding:8px 12px;text-align:left">Vendor</th>
					<th style="padding:8px 12px;text-align:left">Invoice #</th>
					<th style="padding:8px 12px;text-align:left">Due Date</th>
					<th style="padding:8px 12px;text-align:right">Amount</th>
					<th style="padding:8px 12px;text-align:right">Overdue</th>
				</tr>
			</thead>
			<tbody>%s</tbody>
		</table>
		<p style="margin-top:16px;color:#64748b;font-size:13px">Generated %s</p>
	</div>`, len(bills), totalOverdue, strings.Join(rows, "\n"), today)

	e.SendEmailToAdmins(subject, body)
}

// CheckReconciliationAlert sends a weekly email if match rate is below 80%.
func (e *EmailSender) CheckReconciliationAlert() {
	startDate := time.Now().AddDate(0, 0, -30).Format("2006-01-02")
	xeroStart := time.Now().AddDate(0, 0, -31).Format("2006-01-02")
	xeroEnd := time.Now().AddDate(0, 0, 1).Format("2006-01-02")

	var plaidCount int64
	e.GormDB.Raw(`SELECT COUNT(*) FROM transactions WHERE date >= ? AND (pending = false OR pending IS NULL)`, startDate).Scan(&plaidCount)

	if plaidCount == 0 {
		log.Println("Alerts: no Plaid transactions for reconciliation check")
		return
	}

	var matchCount int64
	e.GormDB.Raw(`
		SELECT COUNT(DISTINCT p.id) FROM transactions p
		INNER JOIN xero_bank_transactions x
			ON ABS(x.date - p.date) <= 1
			AND ABS(ABS(p.amount) - x.total) < 0.02
		WHERE p.date >= ? AND (p.pending = false OR p.pending IS NULL)
		  AND x.date >= ? AND x.date <= ?
	`, startDate, xeroStart, xeroEnd).Scan(&matchCount)

	rate := math.Round(float64(matchCount) / float64(plaidCount) * 1000) / 10
	unmatched := plaidCount - matchCount

	if rate >= 80 {
		log.Printf("Alerts: reconciliation rate %.1f%% — OK", rate)
		return
	}

	subject := fmt.Sprintf("ESC Business Analytics — Reconciliation Below 80%% (%.1f%%)", rate)
	body := fmt.Sprintf(`
	<div style="font-family:Arial,sans-serif;max-width:600px">
		<h2 style="color:#b91c1c">Reconciliation Rate: %.1f%%</h2>
		<table style="border-collapse:collapse">
			<tr><td style="padding:4px 12px;font-weight:bold">Total Plaid Transactions:</td><td style="padding:4px 12px">%d</td></tr>
			<tr><td style="padding:4px 12px;font-weight:bold">Matched:</td><td style="padding:4px 12px">%d</td></tr>
			<tr><td style="padding:4px 12px;font-weight:bold">Unmatched:</td><td style="padding:4px 12px;color:#b91c1c">%d</td></tr>
		</table>
		<p style="margin-top:16px"><a href="https://www.businessanalyticsinc.com/reconciliation">View Dashboard</a></p>
		<p style="color:#64748b;font-size:13px">Generated %s</p>
	</div>`, rate, plaidCount, matchCount, unmatched, time.Now().Format("2006-01-02"))

	e.SendEmailToAdmins(subject, body)
}

// CheckLowBankBalance sends an email if any depository account balance is under $5,000.
func (e *EmailSender) CheckLowBankBalance() {
	today := time.Now().Format("2006-01-02")

	type lowBalance struct {
		AccountName      string   `json:"account_name"`
		InstitutionName  string   `json:"institution_name"`
		CurrentBalance   *float64 `json:"current_balance"`
		AvailableBalance *float64 `json:"available_balance"`
	}

	var accounts []lowBalance
	e.GormDB.Raw(`
		SELECT account_name, institution_name, current_balance, available_balance
		FROM daily_balance_snapshots
		WHERE snapshot_date = ?
		  AND LOWER(account_type) = 'depository'
		  AND current_balance IS NOT NULL
		  AND current_balance < 5000
		ORDER BY current_balance ASC
	`, today).Scan(&accounts)

	if len(accounts) == 0 {
		log.Println("Alerts: no low bank balances")
		return
	}

	var rows []string
	for _, a := range accounts {
		cur := 0.0
		if a.CurrentBalance != nil {
			cur = *a.CurrentBalance
		}
		avail := 0.0
		if a.AvailableBalance != nil {
			avail = *a.AvailableBalance
		}

		rows = append(rows, fmt.Sprintf(
			"<tr><td style='padding:6px 12px'>%s</td><td style='padding:6px 12px'>%s</td>"+
				"<td style='padding:6px 12px;text-align:right;color:#b91c1c'>$%.2f</td>"+
				"<td style='padding:6px 12px;text-align:right'>$%.2f</td></tr>",
			a.AccountName, a.InstitutionName, cur, avail))
	}

	subject := fmt.Sprintf("ESC Business Analytics — %d Low Bank Balance Alert(s)", len(accounts))
	body := fmt.Sprintf(`
	<div style="font-family:Arial,sans-serif;max-width:600px">
		<h2 style="color:#b91c1c">Low Bank Balance Alert</h2>
		<p>The following accounts have balances below $5,000:</p>
		<table style="border-collapse:collapse;width:100%%">
			<thead>
				<tr style="background:#f1f5f9">
					<th style="padding:8px 12px;text-align:left">Account</th>
					<th style="padding:8px 12px;text-align:left">Institution</th>
					<th style="padding:8px 12px;text-align:right">Balance</th>
					<th style="padding:8px 12px;text-align:right">Available</th>
				</tr>
			</thead>
			<tbody>%s</tbody>
		</table>
		<p style="margin-top:16px;color:#64748b;font-size:13px">Generated %s</p>
	</div>`, strings.Join(rows, "\n"), today)

	e.SendEmailToAdmins(subject, body)
}
