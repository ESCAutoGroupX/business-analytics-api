package handlers

import "testing"

func TestParseWFFilename(t *testing.T) {
	cases := []struct {
		in      string
		date    string
		vendor  string
		docType string
		wfID    string
		ok      bool
	}{
		{
			in:      "2025-12-23_RSR_AUTO_PARTS_invoice_0e8504.pdf",
			date:    "2025-12-23",
			vendor:  "Rsr Auto Parts",
			docType: "INVOICE",
			wfID:    "0e8504",
			ok:      true,
		},
		{
			in:      "2025-12-11_Carquest_Auto_Parts_statement_799e19.pdf",
			date:    "2025-12-11",
			vendor:  "Carquest Auto Parts",
			docType: "STATEMENT",
			wfID:    "799e19",
			ok:      true,
		},
		{
			in:      "2025-12-12_Aesop_Auto_Parts_rma_ce11f7.pdf",
			date:    "2025-12-12",
			vendor:  "Aesop Auto Parts",
			docType: "CREDIT_MEMO",
			wfID:    "ce11f7",
			ok:      true,
		},
		{
			// 1970-01-01 is the unknown-date sentinel — returned empty
			in:      "1970-01-01_EUROPEAN_MOTOR_CARS_OF_WINDWAR_credit_d79283.pdf",
			date:    "",
			vendor:  "European Motor Cars Of Windwar",
			docType: "CREDIT_MEMO",
			wfID:    "d79283",
			ok:      true,
		},
		{
			// Unknown type word falls back to INVOICE
			in:      "2025-01-15_Some_Vendor_foobar_abcdef.pdf",
			date:    "2025-01-15",
			vendor:  "Some Vendor",
			docType: "INVOICE",
			wfID:    "abcdef",
			ok:      true,
		},
		{in: "too-short.pdf", ok: false},
		{in: "not-a-date_Vendor_invoice_abc.pdf", ok: false},
	}

	for _, c := range cases {
		t.Run(c.in, func(t *testing.T) {
			date, vendor, docType, wfID, ok := parseWFFilename(c.in)
			if ok != c.ok {
				t.Fatalf("ok=%v want %v", ok, c.ok)
			}
			if !c.ok {
				return
			}
			if date != c.date {
				t.Errorf("date=%q want %q", date, c.date)
			}
			if vendor != c.vendor {
				t.Errorf("vendor=%q want %q", vendor, c.vendor)
			}
			if docType != c.docType {
				t.Errorf("docType=%q want %q", docType, c.docType)
			}
			if wfID != c.wfID {
				t.Errorf("wfID=%q want %q", wfID, c.wfID)
			}
		})
	}
}

func TestWFLocationMap(t *testing.T) {
	// ESC_Alpharetta and Alpharetta must resolve to the same code.
	if wfLocationMap["Alpharetta"] != "ALP" || wfLocationMap["ESC_Alpharetta"] != "ALP" {
		t.Errorf("Alpharetta dedupe broken: %q / %q",
			wfLocationMap["Alpharetta"], wfLocationMap["ESC_Alpharetta"])
	}
	if len(wfLocationMap) != 12 {
		t.Errorf("location map size = %d, want 12", len(wfLocationMap))
	}
}
