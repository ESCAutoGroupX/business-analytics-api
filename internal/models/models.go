package models

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"
)

// ──────────────────────────────────────────────
// Users & Auth
// ──────────────────────────────────────────────

type User struct {
	ID                   string     `gorm:"column:id;primaryKey;type:uuid;default:gen_random_uuid()" json:"id"`
	Email                string     `gorm:"column:email;uniqueIndex" json:"email"`
	HashedPassword       string     `gorm:"column:hashed_password" json:"-"`
	FirstName            *string    `gorm:"column:first_name" json:"first_name"`
	LastName             *string    `gorm:"column:last_name" json:"last_name"`
	MobileNumber         *string    `gorm:"column:mobile_number;uniqueIndex" json:"mobile_number"`
	IsActive             bool       `gorm:"column:is_active;default:true" json:"is_active"`
	IsSuperuser          bool       `gorm:"column:is_superuser;default:false" json:"is_superuser"`
	Role                 *string    `gorm:"column:role" json:"role"`
	OTP                  *string    `gorm:"column:otp" json:"-"`
	OTPExpiry            *time.Time `gorm:"column:otp_expiry" json:"-"`
	AuthenticatorSecret  *string    `gorm:"column:authenticator_secret" json:"-"`
	ResetPasswordToken   *string    `gorm:"column:reset_password_token" json:"-"`
	ResetPasswordExpiry  *time.Time `gorm:"column:reset_password_expiry" json:"-"`
	PlaidAccessToken     *string    `gorm:"column:plaid_access_token" json:"-"`
	PlaidCursor          *string    `gorm:"column:plaid_cursor" json:"-"`
	PreferredAuthMethod  *string    `gorm:"column:preferred_auth_method" json:"preferred_auth_method"`
	CreatedAt            time.Time  `gorm:"column:created_at;autoCreateTime" json:"created_at"`
	Locations            []Location `gorm:"many2many:user_location_association;joinForeignKey:user_id;joinReferences:location_id" json:"locations,omitempty"`
}

func (User) TableName() string { return "users" }

type UserLocation struct {
	UserID     string `gorm:"column:user_id;primaryKey" json:"user_id"`
	LocationID int    `gorm:"column:location_id;primaryKey" json:"location_id"`
}

func (UserLocation) TableName() string { return "user_location_association" }

// ──────────────────────────────────────────────
// Plaid Items (multi-item support)
// ──────────────────────────────────────────────

type PlaidItem struct {
	ID              int       `gorm:"primaryKey;autoIncrement" json:"id"`
	UserID          string    `gorm:"column:user_id;not null" json:"user_id"`
	ItemID          string    `gorm:"column:item_id;uniqueIndex;not null" json:"item_id"`
	AccessToken     string    `gorm:"column:access_token;not null" json:"-"`
	Cursor          string    `gorm:"column:cursor" json:"-"`
	InstitutionID   string    `gorm:"column:institution_id" json:"institution_id"`
	InstitutionName string    `gorm:"column:institution_name" json:"institution_name"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

func (PlaidItem) TableName() string { return "plaid_items" }

// ──────────────────────────────────────────────
// Xero Connections
// ──────────────────────────────────────────────

type XeroConnection struct {
	ID           int       `gorm:"primaryKey;autoIncrement" json:"id"`
	UserID       string    `gorm:"column:user_id;not null" json:"user_id"`
	TenantID     string    `gorm:"column:tenant_id;uniqueIndex;not null" json:"tenant_id"`
	TenantName   string    `gorm:"column:tenant_name" json:"tenant_name"`
	AccessToken  string    `gorm:"column:access_token;not null" json:"-"`
	RefreshToken string    `gorm:"column:refresh_token;not null" json:"-"`
	ExpiresAt    time.Time `gorm:"column:token_expires_at;not null" json:"expires_at"`
	Scopes       string    `gorm:"column:scopes" json:"scopes"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

func (XeroConnection) TableName() string { return "xero_connections" }

// ──────────────────────────────────────────────
// Card Location Assignments
// ──────────────────────────────────────────────

type CardLocationAssignment struct {
	ID             int       `gorm:"primaryKey;autoIncrement" json:"id"`
	UserID         string    `gorm:"column:user_id;not null" json:"user_id"`
	CardLast4      string    `gorm:"column:card_last4;not null" json:"card_last4"`
	CardholderName string    `gorm:"column:cardholder_name" json:"cardholder_name"`
	LocationName   string    `gorm:"column:location_name;not null" json:"location_name"`
	PlaidAccountID string    `gorm:"column:plaid_account_id" json:"plaid_account_id"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

func (CardLocationAssignment) TableName() string { return "card_location_assignments" }

// ──────────────────────────────────────────────
// Vendors
// ──────────────────────────────────────────────

type Vendor struct {
	ID                string          `gorm:"column:id;primaryKey;type:uuid;default:gen_random_uuid()" json:"id"`
	Name              string          `gorm:"column:name" json:"name"`
	Category          *string         `gorm:"column:category" json:"category"`
	VendorType        *string         `gorm:"column:vendor_type" json:"vendor_type"`
	ShopName          *string         `gorm:"column:shop_name" json:"shop_name"`
	IsPartsVendor     *string         `gorm:"column:is_parts_vendor" json:"is_parts_vendor"`
	IsCogsVendor      *bool           `gorm:"column:is_cogs_vendor" json:"is_cogs_vendor"`
	IsStatementVendor *bool           `gorm:"column:is_statement_vendor" json:"is_statement_vendor"`
	GLCodeID          *string         `gorm:"column:gl_code_id" json:"gl_code_id"`
	CreatedAt         time.Time       `gorm:"column:created_at;autoCreateTime" json:"created_at"`
	UpdatedAt         time.Time       `gorm:"column:updated_at;autoUpdateTime" json:"updated_at"`
	GLCode            *ChartOfAccount `gorm:"foreignKey:GLCodeID;references:ID" json:"gl_code,omitempty"`
}

func (Vendor) TableName() string { return "vendors" }

// ──────────────────────────────────────────────
// Chart of Accounts (Accounting / GL Codes)
// ──────────────────────────────────────────────

type ChartOfAccount struct {
	ID                string    `gorm:"column:id;primaryKey;type:uuid;default:gen_random_uuid()" json:"id"`
	Code              string    `gorm:"column:code;uniqueIndex" json:"code"`
	Name              *string   `gorm:"column:name" json:"name"`
	AccountType       *string   `gorm:"column:account_type" json:"account_type"`
	Description       *string   `gorm:"column:description" json:"description"`
	ParentID          *string   `gorm:"column:parent_id" json:"parent_id"`
	IsActive          *bool     `gorm:"column:is_active;default:true" json:"is_active"`
	LocationID        *int      `gorm:"column:location_id" json:"location_id"`
	IsPartsVendor     *string   `gorm:"column:is_parts_vendor" json:"is_parts_vendor"`
	IsCogsVendor      *bool     `gorm:"column:is_cogs_vendor" json:"is_cogs_vendor"`
	IsStatementVendor *bool     `gorm:"column:is_statement_vendor" json:"is_statement_vendor"`
	CreatedAt         time.Time `gorm:"column:created_at;autoCreateTime" json:"created_at"`
	UpdatedAt         time.Time `gorm:"column:updated_at;autoUpdateTime" json:"updated_at"`
	Location          *Location `gorm:"foreignKey:LocationID;references:ID" json:"location,omitempty"`
}

func (ChartOfAccount) TableName() string { return "chart_of_accounts" }

// ──────────────────────────────────────────────
// Transactions
// ──────────────────────────────────────────────

type Transaction struct {
	ID                       string     `gorm:"column:id;primaryKey;type:uuid;default:gen_random_uuid()" json:"id"`
	PlaidID                  *string    `gorm:"column:plaid_id" json:"plaid_id"`
	AccountID                *string    `gorm:"column:account_id" json:"account_id"`
	Date                     *string    `gorm:"column:date" json:"date"`
	AuthorizedDate           *string    `gorm:"column:authorized_date" json:"authorized_date"`
	TransactionDatetime      *string    `gorm:"column:transaction_datetime" json:"transaction_datetime"`
	AuthorizedDatetime       *string    `gorm:"column:authorized_datetime" json:"authorized_datetime"`
	Amount                   *float64   `gorm:"column:amount" json:"amount"`
	CurrencyISO              *string    `gorm:"column:currency_iso" json:"currency_iso"`
	CurrencyUnofficial       *string    `gorm:"column:currency_unofficial" json:"currency_unofficial"`
	AvailableBalance         *float64   `gorm:"column:available_balance" json:"available_balance"`
	CurrentBalance           *float64   `gorm:"column:current_balance" json:"current_balance"`
	BalanceLimit             *float64   `gorm:"column:balance_limit" json:"balance_limit"`
	Name                     *string    `gorm:"column:name" json:"name"`
	MerchantName             *string    `gorm:"column:merchant_name" json:"merchant_name"`
	MerchantEntityID         *string    `gorm:"column:merchant_entity_id" json:"merchant_entity_id"`
	Website                  *string    `gorm:"column:website" json:"website"`
	LogoURL                  *string    `gorm:"column:logo_url" json:"logo_url"`
	Category                 *string    `gorm:"column:category;type:jsonb" json:"category"`
	CategoryID               *string    `gorm:"column:category_id" json:"category_id"`
	PersonalFinanceCategory  *string    `gorm:"column:personal_finance_category;type:jsonb" json:"personal_finance_category"`
	PFCIconURL               *string    `gorm:"column:pfc_icon_url" json:"pfc_icon_url"`
	PaymentChannel           *string    `gorm:"column:payment_channel" json:"payment_channel"`
	TransactionType          *string    `gorm:"column:transaction_type" json:"transaction_type"`
	TransactionCode          *string    `gorm:"column:transaction_code" json:"transaction_code"`
	RunningBalance           *float64   `gorm:"column:running_balance" json:"running_balance"`
	Pending                  *bool      `gorm:"column:pending" json:"pending"`
	PendingID                *string    `gorm:"column:pending_id" json:"pending_id"`
	Location                 *string    `gorm:"column:location;type:jsonb" json:"location"`
	Counterparties           *string    `gorm:"column:counterparties;type:jsonb" json:"counterparties"`
	PaymentMeta              *string    `gorm:"column:payment_meta;type:jsonb" json:"payment_meta"`
	AccountOwner             *string    `gorm:"column:account_owner" json:"account_owner"`
	CheckNumber              *string    `gorm:"column:check_number" json:"check_number"`
	Vendor                   *string    `gorm:"column:vendor" json:"vendor"`
	Source                   *string    `gorm:"column:source" json:"source"`
	LinkedDocument           *string    `gorm:"column:linked_document" json:"linked_document"`
	CreatedBy                *string    `gorm:"column:created_by" json:"created_by"`
	Documents                *string    `gorm:"column:documents;type:jsonb" json:"documents"`
	IsDuplicated             *bool      `gorm:"column:is_duplicated" json:"is_duplicated"`
	IsAcknowledged           *bool      `gorm:"column:is_acknowledged" json:"is_acknowledged"`
	AccountType              *string    `gorm:"column:account_type" json:"account_type"`
	AccountSubtype           *string    `gorm:"column:account_subtype" json:"account_subtype"`
	AccountName              *string    `gorm:"column:account_name" json:"account_name"`
	RONumber                 *int       `gorm:"column:ro_number" json:"ro_number"`
	ROID                     *string    `gorm:"column:ro_id" json:"ro_id"`
	PaymentMethodID          *string    `gorm:"column:payment_method_id" json:"payment_method_id"`
	GLCodeID                 *string    `gorm:"column:gl_code_id" json:"gl_code_id"`
	UserID                   *string    `gorm:"column:user_id" json:"user_id"`
	CreatedAt                time.Time  `gorm:"column:created_at;autoCreateTime" json:"created_at"`
	UpdatedAt                time.Time  `gorm:"column:updated_at;autoUpdateTime" json:"updated_at"`
}

func (Transaction) TableName() string { return "transactions" }

type TransactionChangeLog struct {
	ID            string    `gorm:"column:id;primaryKey;type:uuid;default:gen_random_uuid()" json:"id"`
	TransactionID string    `gorm:"column:transaction_id" json:"transaction_id"`
	OriginalData  *string   `gorm:"column:original_data;type:jsonb" json:"original_data"`
	EditedData    *string   `gorm:"column:edited_data;type:jsonb" json:"edited_data"`
	ChangedBy     *string   `gorm:"column:changed_by" json:"changed_by"`
	ChangedAt     time.Time `gorm:"column:changed_at" json:"changed_at"`
}

func (TransactionChangeLog) TableName() string { return "transaction_change_logs" }

// ──────────────────────────────────────────────
// Payment Methods
// ──────────────────────────────────────────────

type PaymentMethod struct {
	ID                     string     `gorm:"column:id;primaryKey;type:uuid;default:gen_random_uuid()" json:"id"`
	PlaidAccountID         *string    `gorm:"column:plaid_account_id" json:"plaid_account_id"`
	Title                  *string    `gorm:"column:title" json:"title"`
	Description            *string    `gorm:"column:description" json:"description"`
	MethodType             string     `gorm:"column:method_type" json:"method_type"`
	ChequeSeriesStart      *string    `gorm:"column:cheque_series_start" json:"cheque_series_start"`
	ChequeSeriesEnd        *string    `gorm:"column:cheque_series_end" json:"cheque_series_end"`
	BankName               *string    `gorm:"column:bank_name" json:"bank_name"`
	BankAccountNumber      *string    `gorm:"column:bank_account_number" json:"bank_account_number"`
	BankRoutingNumber      *string    `gorm:"column:bank_routing_number" json:"bank_routing_number"`
	CardLast4Digits        *string    `gorm:"column:card_last_4_digits" json:"card_last_4_digits"`
	CardName               *string    `gorm:"column:card_name" json:"card_name"`
	Subtype                *string    `gorm:"column:subtype" json:"subtype"`
	HolderCategory         *string    `gorm:"column:holder_category" json:"holder_category"`
	BalanceAvailable       *float64   `gorm:"column:balance_available" json:"balance_available"`
	BalanceCurrent         *float64   `gorm:"column:balance_current" json:"balance_current"`
	CreditLimit            *float64   `gorm:"column:credit_limit" json:"credit_limit"`
	IsoCurrencyCode        *string    `gorm:"column:iso_currency_code" json:"iso_currency_code"`
	UnofficialCurrencyCode *string    `gorm:"column:unofficial_currency_code" json:"unofficial_currency_code"`
	TotalAmount            *float64   `gorm:"column:total_amount" json:"total_amount"`
	MinimumBalance         *float64   `gorm:"column:minimum_balance" json:"minimum_balance"`
	StartingCreditCardBal  *float64   `gorm:"column:starting_credit_card_bal" json:"starting_credit_card_bal"`
	NextPaymentDueDate     *time.Time `gorm:"column:next_payment_due_date" json:"next_payment_due_date"`
	IsBalanceUpdated       *bool      `gorm:"column:is_balance_updated" json:"is_balance_updated"`
	SortingOrder           *int       `gorm:"column:sorting_order" json:"sorting_order"`
	LocationID             *string    `gorm:"column:location_id" json:"location_id"`
	CreatedAt              time.Time  `gorm:"column:created_at;autoCreateTime" json:"created_at"`
	UpdatedAt              time.Time  `gorm:"column:updated_at;autoUpdateTime" json:"updated_at"`
}

func (PaymentMethod) TableName() string { return "payment_methods" }

// ──────────────────────────────────────────────
// Locations & Shop Info
// ──────────────────────────────────────────────

type Location struct {
	ID            int        `gorm:"column:id;primaryKey;autoIncrement" json:"id"`
	LocationName  string     `gorm:"column:location_name" json:"location_name"`
	AddressLine1  *string    `gorm:"column:address_line_1" json:"address_line_1"`
	AddressLine2  *string    `gorm:"column:address_line_2" json:"address_line_2"`
	City          *string    `gorm:"column:city" json:"city"`
	StateProvince *string    `gorm:"column:state_province" json:"state_province"`
	PostalCode    *string    `gorm:"column:postal_code" json:"postal_code"`
	Country       *string    `gorm:"column:country" json:"country"`
	ShopID        *int       `gorm:"column:shop_id" json:"shop_id"`
	CreatedAt     time.Time  `gorm:"column:created_at;autoCreateTime" json:"created_at"`
	UpdatedAt     time.Time  `gorm:"column:updated_at;autoUpdateTime" json:"updated_at"`
	ShopInfo      *ShopInfo  `gorm:"foreignKey:ShopID;references:ID" json:"shop_info,omitempty"`
}

func (Location) TableName() string { return "locations" }

type ShopInfo struct {
	ID                 int       `gorm:"column:id;primaryKey;autoIncrement" json:"id"`
	ShopName           *string   `gorm:"column:shop_name" json:"shop_name"`
	ContactEmail       *string   `gorm:"column:contact_email" json:"contact_email"`
	PDFForwardingEmail *string   `gorm:"column:pdf_forwarding_email" json:"pdf_forwarding_email"`
	CreatedAt          time.Time `gorm:"column:created_at;autoCreateTime" json:"created_at"`
	UpdatedAt          time.Time `gorm:"column:updated_at;autoUpdateTime" json:"updated_at"`
}

func (ShopInfo) TableName() string { return "shop_info" }

// ──────────────────────────────────────────────
// Assets
// ──────────────────────────────────────────────

type Asset struct {
	ID                               int       `gorm:"column:id;primaryKey;autoIncrement" json:"id"`
	AssetID                          string    `gorm:"column:asset_id" json:"asset_id"`
	AssetName                        string    `gorm:"column:asset_name" json:"asset_name"`
	Description                      *string   `gorm:"column:description" json:"description"`
	OriginalCost                     float64   `gorm:"column:original_cost" json:"original_cost"`
	SalvageValue                     *float64  `gorm:"column:salvage_value;default:0" json:"salvage_value"`
	UsefulLife                       *int      `gorm:"column:useful_life" json:"useful_life"`
	InitialAccumulatedDepreciation   *float64  `gorm:"column:initial_accumulated_depreciation;default:0" json:"initial_accumulated_depreciation"`
	AssetCategory                    *string   `gorm:"column:asset_category;default:'Other'" json:"asset_category"`
	FixedAssetAccount                *string   `gorm:"column:fixed_asset_account" json:"fixed_asset_account"`
	AccumulatedDepreciationAccount   *string   `gorm:"column:accumulated_depreciation_account" json:"accumulated_depreciation_account"`
	DepreciationExpenseAccount       *string   `gorm:"column:depreciation_expense_account" json:"depreciation_expense_account"`
	Method                           *string   `gorm:"column:method;default:'Straight-Line'" json:"method"`
	DepreciationFrequency            *string   `gorm:"column:depreciation_frequency;default:'Monthly'" json:"depreciation_frequency"`
	DepreciationConvention           *string   `gorm:"column:depreciation_convention;default:'Full-Month'" json:"depreciation_convention"`
	AcquisitionDate                  *string   `gorm:"column:acquisition_date" json:"acquisition_date"`
	StartDate                        *string   `gorm:"column:start_date" json:"start_date"`
	EndDate                          *string   `gorm:"column:end_date" json:"end_date"`
	LocationField                    *string   `gorm:"column:location" json:"location"`
	VendorID                         *string   `gorm:"column:vendor_id" json:"vendor_id"`
	CreatedAt                        time.Time `gorm:"column:created_at;autoCreateTime" json:"created_at"`
	UpdatedAt                        time.Time `gorm:"column:updated_at;autoUpdateTime" json:"updated_at"`
}

func (Asset) TableName() string { return "assets" }

// ──────────────────────────────────────────────
// Cards
// ──────────────────────────────────────────────

type Card struct {
	ID              int       `gorm:"column:id;primaryKey;autoIncrement" json:"id"`
	CardName        string    `gorm:"column:card_name" json:"card_name"`
	CardIDPlaid     *string   `gorm:"column:card_id_plaid;uniqueIndex" json:"card_id_plaid"`
	BillingStartDay *int      `gorm:"column:billing_start_day" json:"billing_start_day"`
	BillingEndDay   *int      `gorm:"column:billing_end_day" json:"billing_end_day"`
	CycleType       *string   `gorm:"column:cycle_type" json:"cycle_type"`
	LastFourDigits  *string   `gorm:"column:last_four_digits" json:"last_four_digits"`
	BankProvider    *string   `gorm:"column:bank_provider" json:"bank_provider"`
	CreatedAt       time.Time `gorm:"column:created_at;autoCreateTime" json:"created_at"`
	UpdatedAt       time.Time `gorm:"column:updated_at;autoUpdateTime" json:"updated_at"`
}

func (Card) TableName() string { return "cards" }

// ──────────────────────────────────────────────
// Pay Bills
// ──────────────────────────────────────────────

type PayBill struct {
	ID                int               `gorm:"column:id;primaryKey;autoIncrement" json:"id"`
	VendorID          string            `gorm:"column:vendor_id" json:"vendor_id"`
	Amount            float64           `gorm:"column:amount" json:"amount"`
	Date              *string           `gorm:"column:date" json:"date"`
	Category          *string           `gorm:"column:category" json:"category"`
	InvoiceURL        *string           `gorm:"column:invoice_url" json:"invoice_url"`
	PaymentMethodID   *string           `gorm:"column:payment_method_id" json:"payment_method_id"`
	ChequeNumber      *string           `gorm:"column:cheque_number" json:"cheque_number"`
	ChequeClearingDate *string          `gorm:"column:cheque_clearing_date" json:"cheque_clearing_date"`
	SpentAs           *string           `gorm:"column:spent_as" json:"spent_as"`
	Reference         *string           `gorm:"column:reference" json:"reference"`
	PaidByCheck       *bool             `gorm:"column:paid_by_check" json:"paid_by_check"`
	UserID            *string           `gorm:"column:user_id" json:"user_id"`
	CreatedAt         time.Time         `gorm:"column:created_at;autoCreateTime" json:"created_at"`
	UpdatedAt         time.Time         `gorm:"column:updated_at;autoUpdateTime" json:"updated_at"`
	LineItems         []PayBillLineItem `gorm:"foreignKey:PaybillID;references:ID" json:"line_items"`
	SchedulePayments  []SchedulePayment `gorm:"foreignKey:PaybillID;references:ID" json:"schedule_payments"`
}

func (PayBill) TableName() string { return "pay_bills" }

type PayBillLineItem struct {
	ID          int      `gorm:"column:id;primaryKey;autoIncrement" json:"id"`
	PaybillID   int      `gorm:"column:paybill_id" json:"paybill_id"`
	Item        *string  `gorm:"column:item" json:"item"`
	Description *string  `gorm:"column:description" json:"description"`
	Qty         *float64 `gorm:"column:qty" json:"qty"`
	UnitPrice   *float64 `gorm:"column:unit_price" json:"unit_price"`
	TotalAmount *float64 `gorm:"column:total_amount" json:"total_amount"`
	GLCodeID    *string  `gorm:"column:gl_code_id" json:"gl_code_id"`
	LocationID  *int     `gorm:"column:location_id" json:"location_id"`
}

func (PayBillLineItem) TableName() string { return "pay_bill_line_items" }

type SchedulePayment struct {
	ID          int        `gorm:"column:id;primaryKey;autoIncrement" json:"id"`
	PaybillID   int        `gorm:"column:paybill_id" json:"paybill_id"`
	RepeatEvery *int       `gorm:"column:repeat_every" json:"repeat_every"`
	Frequency   *string    `gorm:"column:frequency" json:"frequency"`
	StartDate   *string    `gorm:"column:start_date" json:"start_date"`
	EndDate     *string    `gorm:"column:end_date" json:"end_date"`
	Enabled     *bool      `gorm:"column:enabled" json:"enabled"`
	CreatedAt   time.Time  `gorm:"column:created_at;autoCreateTime" json:"created_at"`
	UpdatedAt   time.Time  `gorm:"column:updated_at;autoUpdateTime" json:"updated_at"`
	Reminders   []Reminder `gorm:"foreignKey:ScheduledPaymentID;references:ID" json:"reminders,omitempty"`
}

func (SchedulePayment) TableName() string { return "schedule_payments" }

type Reminder struct {
	ID                 int       `gorm:"column:id;primaryKey;autoIncrement" json:"id"`
	ScheduledPaymentID int       `gorm:"column:scheduled_payment_id" json:"scheduled_payment_id"`
	ReminderType       *string   `gorm:"column:reminder_type" json:"reminder_type"`
	Message            *string   `gorm:"column:message" json:"message"`
	ReminderDate       *string   `gorm:"column:reminder_date" json:"reminder_date"`
	Acknowledged       *bool     `gorm:"column:acknowledged;default:false" json:"acknowledged"`
	CreatedAt          time.Time `gorm:"column:created_at;autoCreateTime" json:"created_at"`
	UpdatedAt          time.Time `gorm:"column:updated_at;autoUpdateTime" json:"updated_at"`
}

func (Reminder) TableName() string { return "reminders" }

type ManualBill struct {
	ID                 int       `gorm:"column:id;primaryKey;autoIncrement" json:"id"`
	VendorID           *string   `gorm:"column:vendor_id" json:"vendor_id"`
	Amount             *float64  `gorm:"column:amount" json:"amount"`
	DueDate            *string   `gorm:"column:due_date" json:"due_date"`
	PayableType        *string   `gorm:"column:payable_type" json:"payable_type"`
	GLCodeID           *string   `gorm:"column:gl_code_id" json:"gl_code_id"`
	StatementPeriod    *string   `gorm:"column:statement_period" json:"statement_period"`
	StatementStartDate *string   `gorm:"column:statement_start_date" json:"statement_start_date"`
	StatementEndDate   *string   `gorm:"column:statement_end_date" json:"statement_end_date"`
	Notes              *string   `gorm:"column:notes" json:"notes"`
	Status             *string   `gorm:"column:status" json:"status"`
	UserID             *string   `gorm:"column:user_id" json:"user_id"`
	CreatedAt          time.Time `gorm:"column:created_at;autoCreateTime" json:"created_at"`
	UpdatedAt          time.Time `gorm:"column:updated_at;autoUpdateTime" json:"updated_at"`
}

func (ManualBill) TableName() string { return "manual_bill_entries" }

// ──────────────────────────────────────────────
// Payroll
// ──────────────────────────────────────────────

type PayrollEntry struct {
	ID           int       `gorm:"column:id;primaryKey;autoIncrement" json:"id"`
	Date         string    `gorm:"column:date" json:"date"`
	EmployeeName string    `gorm:"column:employee_name" json:"employee_name"`
	GLCode       string    `gorm:"column:gl_code" json:"gl_code"`
	Description  *string   `gorm:"column:description" json:"description"`
	GrossPay     float64   `gorm:"column:gross_pay" json:"gross_pay"`
	Taxes        float64   `gorm:"column:taxes" json:"taxes"`
	NetPay       float64   `gorm:"column:net_pay" json:"net_pay"`
	LocationID   int       `gorm:"column:location_id" json:"location_id"`
	UserID       *string   `gorm:"column:user_id" json:"user_id"`
	CreatedAt    time.Time `gorm:"column:created_at;autoCreateTime" json:"created_at"`
	UpdatedAt    time.Time `gorm:"column:updated_at;autoUpdateTime" json:"updated_at"`
	Location     *Location `gorm:"foreignKey:LocationID;references:ID" json:"location,omitempty"`
}

func (PayrollEntry) TableName() string { return "payroll" }

type PayrollAdjustment struct {
	ID           int       `gorm:"column:id;primaryKey;autoIncrement" json:"id"`
	Date         string    `gorm:"column:date" json:"date"`
	GLCode       string    `gorm:"column:gl_code" json:"gl_code"`
	Description  *string   `gorm:"column:description" json:"description"`
	Debit        *float64  `gorm:"column:debit" json:"debit"`
	Credit       *float64  `gorm:"column:credit" json:"credit"`
	LocationID   *int      `gorm:"column:location_id" json:"location_id"`
	UserID       *string   `gorm:"column:user_id" json:"user_id"`
	Notes        *string   `gorm:"column:notes" json:"notes"`
	EmployeeName *string   `gorm:"column:employee_name" json:"employee_name"`
	CreatedAt    time.Time `gorm:"column:created_at;autoCreateTime" json:"created_at"`
	UpdatedAt    time.Time `gorm:"column:updated_at;autoUpdateTime" json:"updated_at"`
}

func (PayrollAdjustment) TableName() string { return "payroll_account_adjustments" }

// ──────────────────────────────────────────────
// Permissions & Roles
// ──────────────────────────────────────────────

type Permission struct {
	ID          int    `gorm:"column:id;primaryKey;autoIncrement" json:"id"`
	Keyword     string `gorm:"column:keyword;uniqueIndex" json:"keyword"`
	Endpoint    string `gorm:"column:endpoint" json:"endpoint"`
	Method      string `gorm:"column:method" json:"method"`
	Description string `gorm:"column:description" json:"description"`
	IsActive    bool   `gorm:"column:is_active;default:true" json:"is_active"`
}

func (Permission) TableName() string { return "permissions" }

type Role struct {
	ID          int          `gorm:"column:id;primaryKey;autoIncrement" json:"id"`
	Name        string       `gorm:"column:name;uniqueIndex" json:"name"`
	Description string       `gorm:"column:description" json:"description"`
	IsActive    bool         `gorm:"column:is_active;default:true" json:"is_active"`
	Permissions []Permission `gorm:"many2many:role_permissions;joinForeignKey:role_id;joinReferences:permission_id" json:"permissions,omitempty"`
}

func (Role) TableName() string { return "roles" }

type RolePermission struct {
	RoleID       int `gorm:"column:role_id;primaryKey" json:"role_id"`
	PermissionID int `gorm:"column:permission_id;primaryKey" json:"permission_id"`
}

func (RolePermission) TableName() string { return "role_permissions" }

// ──────────────────────────────────────────────
// JSONB helper type for PostgreSQL JSONB columns
// ──────────────────────────────────────────────

type JSONB json.RawMessage

func (j JSONB) Value() (driver.Value, error) {
	if len(j) == 0 {
		return nil, nil
	}
	return string(j), nil
}

func (j *JSONB) Scan(value interface{}) error {
	if value == nil {
		*j = JSONB("null")
		return nil
	}
	switch v := value.(type) {
	case []byte:
		*j = append((*j)[0:0], v...)
	case string:
		*j = JSONB(v)
	default:
		return fmt.Errorf("unsupported type for JSONB: %T", value)
	}
	return nil
}

func (j JSONB) MarshalJSON() ([]byte, error) {
	if len(j) == 0 {
		return []byte("null"), nil
	}
	return []byte(j), nil
}

func (j *JSONB) UnmarshalJSON(data []byte) error {
	*j = append((*j)[0:0], data...)
	return nil
}

func (JSONB) GormDataType() string { return "jsonb" }

// ──────────────────────────────────────────────
// Xero Synced Data
// ──────────────────────────────────────────────

type XeroBankTransaction struct {
	ID              int        `gorm:"primaryKey;autoIncrement" json:"id"`
	XeroID          string     `gorm:"column:xero_id;uniqueIndex;not null" json:"xero_id"`
	TenantID        string     `gorm:"column:tenant_id;not null" json:"tenant_id"`
	Type            *string    `gorm:"column:type" json:"type"`
	ContactID       *string    `gorm:"column:contact_id" json:"contact_id"`
	ContactName     *string    `gorm:"column:contact_name" json:"contact_name"`
	BankAccountID   *string    `gorm:"column:bank_account_id" json:"bank_account_id"`
	BankAccountName *string    `gorm:"column:bank_account_name" json:"bank_account_name"`
	Date            *time.Time `gorm:"column:date" json:"date"`
	Reference       *string    `gorm:"column:reference" json:"reference"`
	Status          *string    `gorm:"column:status" json:"status"`
	SubTotal        *float64   `gorm:"column:sub_total" json:"sub_total"`
	TotalTax        *float64   `gorm:"column:total_tax" json:"total_tax"`
	Total           *float64   `gorm:"column:total" json:"total"`
	IsReconciled    bool       `gorm:"column:is_reconciled;default:false" json:"is_reconciled"`
	LineItems       JSONB      `gorm:"column:line_items" json:"line_items"`
	UpdatedDateUTC  *time.Time `gorm:"column:updated_date_utc" json:"updated_date_utc"`
	SyncedAt        time.Time  `gorm:"column:synced_at;autoCreateTime" json:"synced_at"`
}

func (XeroBankTransaction) TableName() string { return "xero_bank_transactions" }

type XeroInvoice struct {
	ID             int        `gorm:"primaryKey;autoIncrement" json:"id"`
	XeroID         string     `gorm:"column:xero_id;uniqueIndex;not null" json:"xero_id"`
	TenantID       string     `gorm:"column:tenant_id;not null" json:"tenant_id"`
	Type           *string    `gorm:"column:type" json:"type"`
	ContactID      *string    `gorm:"column:contact_id" json:"contact_id"`
	ContactName    *string    `gorm:"column:contact_name" json:"contact_name"`
	InvoiceNumber  *string    `gorm:"column:invoice_number" json:"invoice_number"`
	Reference      *string    `gorm:"column:reference" json:"reference"`
	Date           *time.Time `gorm:"column:date" json:"date"`
	DueDate        *time.Time `gorm:"column:due_date" json:"due_date"`
	Status         *string    `gorm:"column:status" json:"status"`
	SubTotal       *float64   `gorm:"column:sub_total" json:"sub_total"`
	TotalTax       *float64   `gorm:"column:total_tax" json:"total_tax"`
	Total          *float64   `gorm:"column:total" json:"total"`
	AmountDue      *float64   `gorm:"column:amount_due" json:"amount_due"`
	AmountPaid     *float64   `gorm:"column:amount_paid" json:"amount_paid"`
	AmountCredited *float64   `gorm:"column:amount_credited" json:"amount_credited"`
	LineItems      JSONB      `gorm:"column:line_items" json:"line_items"`
	UpdatedDateUTC *time.Time `gorm:"column:updated_date_utc" json:"updated_date_utc"`
	SyncedAt       time.Time  `gorm:"column:synced_at;autoCreateTime" json:"synced_at"`
}

func (XeroInvoice) TableName() string { return "xero_invoices" }

type XeroContact struct {
	ID             int        `gorm:"primaryKey;autoIncrement" json:"id"`
	XeroID         string     `gorm:"column:xero_id;uniqueIndex;not null" json:"xero_id"`
	TenantID       string     `gorm:"column:tenant_id;not null" json:"tenant_id"`
	Name           string     `gorm:"column:name;not null" json:"name"`
	FirstName      *string    `gorm:"column:first_name" json:"first_name"`
	LastName       *string    `gorm:"column:last_name" json:"last_name"`
	Email          *string    `gorm:"column:email" json:"email"`
	Phone          *string    `gorm:"column:phone" json:"phone"`
	AccountNumber  *string    `gorm:"column:account_number" json:"account_number"`
	TaxNumber      *string    `gorm:"column:tax_number" json:"tax_number"`
	IsSupplier     bool       `gorm:"column:is_supplier;default:false" json:"is_supplier"`
	IsCustomer     bool       `gorm:"column:is_customer;default:false" json:"is_customer"`
	ContactStatus  *string    `gorm:"column:contact_status" json:"contact_status"`
	UpdatedDateUTC *time.Time `gorm:"column:updated_date_utc" json:"updated_date_utc"`
	SyncedAt       time.Time  `gorm:"column:synced_at;autoCreateTime" json:"synced_at"`
}

func (XeroContact) TableName() string { return "xero_contacts" }

type XeroPayment struct {
	ID             int        `gorm:"primaryKey;autoIncrement" json:"id"`
	XeroID         string     `gorm:"column:xero_id;uniqueIndex;not null" json:"xero_id"`
	TenantID       string     `gorm:"column:tenant_id;not null" json:"tenant_id"`
	InvoiceID      *string    `gorm:"column:invoice_id" json:"invoice_id"`
	AccountID      *string    `gorm:"column:account_id" json:"account_id"`
	Date           *time.Time `gorm:"column:date" json:"date"`
	Amount         *float64   `gorm:"column:amount" json:"amount"`
	Reference      *string    `gorm:"column:reference" json:"reference"`
	Status         *string    `gorm:"column:status" json:"status"`
	PaymentType    *string    `gorm:"column:payment_type" json:"payment_type"`
	UpdatedDateUTC *time.Time `gorm:"column:updated_date_utc" json:"updated_date_utc"`
	SyncedAt       time.Time  `gorm:"column:synced_at;autoCreateTime" json:"synced_at"`
}

func (XeroPayment) TableName() string { return "xero_payments" }

type XeroAsset struct {
	ID                       int        `gorm:"primaryKey;autoIncrement" json:"id"`
	XeroID                   string     `gorm:"column:xero_id;uniqueIndex" json:"xero_id"`
	TenantID                 string     `gorm:"column:tenant_id;not null" json:"tenant_id"`
	AssetName                string     `gorm:"column:asset_name;not null" json:"asset_name"`
	AssetNumber              *string    `gorm:"column:asset_number;uniqueIndex" json:"asset_number"`
	AssetTypeID              *string    `gorm:"column:asset_type_id" json:"asset_type_id"`
	AssetTypeName            *string    `gorm:"column:asset_type_name" json:"asset_type_name"`
	Status                   *string    `gorm:"column:status" json:"status"`
	PurchaseDate             *time.Time `gorm:"column:purchase_date" json:"purchase_date"`
	PurchasePrice            *float64   `gorm:"column:purchase_price" json:"purchase_price"`
	DisposalDate             *time.Time `gorm:"column:disposal_date" json:"disposal_date"`
	DisposalPrice            *float64   `gorm:"column:disposal_price" json:"disposal_price"`
	DepreciationMethod       *string    `gorm:"column:depreciation_method" json:"depreciation_method"`
	AveragingMethod          *string    `gorm:"column:averaging_method" json:"averaging_method"`
	DepreciationRate         *float64   `gorm:"column:depreciation_rate" json:"depreciation_rate"`
	EffectiveLifeYears       *int       `gorm:"column:effective_life_years" json:"effective_life_years"`
	CostLimit                *float64   `gorm:"column:cost_limit" json:"cost_limit"`
	ResidualValue            *float64   `gorm:"column:residual_value" json:"residual_value"`
	BookValue                *float64   `gorm:"column:book_value" json:"book_value"`
	CurrentAccumDepreciation *float64   `gorm:"column:current_accum_depreciation" json:"current_accum_depreciation"`
	PriorAccumDepreciation   *float64   `gorm:"column:prior_accum_depreciation" json:"prior_accum_depreciation"`
	CurrentDepreciation      *float64   `gorm:"column:current_depreciation" json:"current_depreciation"`
	UpdatedDateUTC           *time.Time `gorm:"column:updated_date_utc" json:"updated_date_utc"`
	Description              *string    `gorm:"column:description" json:"description"`
	Location                 *string    `gorm:"column:location" json:"location"`
	DepreciationStartDate    *time.Time `gorm:"column:depreciation_start_date" json:"depreciation_start_date"`
	AssetType                string     `gorm:"column:asset_type" json:"asset_type"`
	EffectiveLife            *float64   `gorm:"column:effective_life" json:"effective_life"`
	AccumulatedDepreciation  *float64   `gorm:"column:accumulated_depreciation" json:"accumulated_depreciation"`
	UsefulLifeYearsOverride    *int       `gorm:"column:useful_life_years_override" json:"useful_life_years_override"`
	DepreciationMethodOverride *string    `gorm:"column:depreciation_method_override" json:"depreciation_method_override"`
	DepreciationRateOverride   *float64   `gorm:"column:depreciation_rate_override" json:"depreciation_rate_override"`
	AssetCategory            *string    `gorm:"column:asset_category" json:"asset_category"`
	AIClassified             bool       `gorm:"column:ai_classified;default:false" json:"ai_classified"`
	AIConfidence             *float64   `gorm:"column:ai_confidence" json:"ai_confidence"`
	AIReasoning              *string    `gorm:"column:ai_reasoning" json:"ai_reasoning"`
	SyncedAt                 time.Time  `gorm:"column:synced_at;autoCreateTime" json:"synced_at"`
}

func (XeroAsset) TableName() string { return "xero_assets" }

type XeroAssetType struct {
	ID                               int      `gorm:"primaryKey;autoIncrement" json:"id"`
	XeroID                           string   `gorm:"column:xero_id;uniqueIndex;not null" json:"xero_id"`
	TenantID                         string   `gorm:"column:tenant_id;not null" json:"tenant_id"`
	AssetTypeName                    string   `gorm:"column:asset_type_name;not null" json:"asset_type_name"`
	FixedAssetAccountID              *string  `gorm:"column:fixed_asset_account_id" json:"fixed_asset_account_id"`
	DepreciationExpenseAccountID     *string  `gorm:"column:depreciation_expense_account_id" json:"depreciation_expense_account_id"`
	AccumulatedDepreciationAccountID *string  `gorm:"column:accumulated_depreciation_account_id" json:"accumulated_depreciation_account_id"`
	DepreciationMethod               *string  `gorm:"column:depreciation_method" json:"depreciation_method"`
	AveragingMethod                  *string  `gorm:"column:averaging_method" json:"averaging_method"`
	DepreciationRate                 *float64 `gorm:"column:depreciation_rate" json:"depreciation_rate"`
	EffectiveLifeYears               *int     `gorm:"column:effective_life_years" json:"effective_life_years"`
	SyncedAt                         time.Time `gorm:"column:synced_at;autoCreateTime" json:"synced_at"`
}

func (XeroAssetType) TableName() string { return "xero_asset_types" }

type XeroAccount struct {
	ID                 int       `gorm:"primaryKey;autoIncrement" json:"id"`
	XeroID             string    `gorm:"column:xero_id;uniqueIndex;not null" json:"xero_id"`
	TenantID           string    `gorm:"column:tenant_id" json:"tenant_id"`
	Code               string    `gorm:"column:code;index" json:"code"`
	Name               string    `gorm:"column:name" json:"name"`
	Type               string    `gorm:"column:type" json:"type"`
	Class              string    `gorm:"column:class" json:"class"`
	Status             string    `gorm:"column:status" json:"status"`
	Description        string    `gorm:"column:description" json:"description"`
	EnablePayments     bool      `gorm:"column:enable_payments" json:"enable_payments"`
	ShowInExpenseClaims bool     `gorm:"column:show_in_expense_claims" json:"show_in_expense_claims"`
	BankAccountNumber  string    `gorm:"column:bank_account_number" json:"bank_account_number"`
	CurrencyCode       string    `gorm:"column:currency_code" json:"currency_code"`
	TaxType            string    `gorm:"column:tax_type" json:"tax_type"`
	SyncedAt           time.Time `gorm:"column:synced_at;autoCreateTime" json:"synced_at"`
}

func (XeroAccount) TableName() string { return "xero_accounts" }

type XeroJournal struct {
	ID             int        `gorm:"primaryKey;autoIncrement" json:"id"`
	XeroID         string     `gorm:"column:xero_id;uniqueIndex;not null" json:"xero_id"`
	TenantID       string     `gorm:"column:tenant_id;not null" json:"tenant_id"`
	JournalDate    *time.Time `gorm:"column:journal_date" json:"journal_date"`
	JournalNumber  *int       `gorm:"column:journal_number" json:"journal_number"`
	SourceID       *string    `gorm:"column:source_id" json:"source_id"`
	SourceType     *string    `gorm:"column:source_type" json:"source_type"`
	Reference      *string    `gorm:"column:reference" json:"reference"`
	JournalLines   JSONB      `gorm:"column:journal_lines" json:"journal_lines"`
	CreatedDateUTC *time.Time `gorm:"column:created_date_utc" json:"created_date_utc"`
	SyncedAt       time.Time  `gorm:"column:synced_at;autoCreateTime" json:"synced_at"`
}

func (XeroJournal) TableName() string { return "xero_journals" }

type XeroTrackingCategory struct {
	ID       int       `gorm:"primaryKey;autoIncrement" json:"id"`
	XeroID   string    `gorm:"column:xero_id;uniqueIndex;not null" json:"xero_id"`
	TenantID string    `gorm:"column:tenant_id;not null" json:"tenant_id"`
	Name     string    `gorm:"column:name;not null" json:"name"`
	Status   *string   `gorm:"column:status" json:"status"`
	Options  JSONB     `gorm:"column:options" json:"options"`
	SyncedAt time.Time `gorm:"column:synced_at;autoCreateTime" json:"synced_at"`
}

func (XeroTrackingCategory) TableName() string { return "xero_tracking_categories" }

type XeroSyncLog struct {
	ID            int       `gorm:"primaryKey;autoIncrement" json:"id"`
	TenantID      string    `gorm:"column:tenant_id;not null" json:"tenant_id"`
	Endpoint      string    `gorm:"column:endpoint;not null" json:"endpoint"`
	LastSyncAt    time.Time `gorm:"column:last_sync_at;autoCreateTime" json:"last_sync_at"`
	RecordsSynced int       `gorm:"column:records_synced;default:0" json:"records_synced"`
	Status        string    `gorm:"column:status;default:'success'" json:"status"`
	ErrorMessage  string    `gorm:"column:error_message" json:"error_message"`
	CreatedAt     time.Time `gorm:"column:created_at;autoCreateTime" json:"created_at"`
}

func (XeroSyncLog) TableName() string { return "xero_sync_log" }

type XeroSyncState struct {
	Endpoint           string     `gorm:"column:endpoint;primaryKey" json:"endpoint"`
	LastSyncAt         *time.Time `gorm:"column:last_sync_at" json:"last_sync_at"`
	LastSuccessfulAt   *time.Time `gorm:"column:last_successful_at" json:"last_successful_at"`
	TotalRecordsSynced int        `gorm:"column:total_records_synced;default:0" json:"total_records_synced"`
	DailyCallCount     int        `gorm:"column:daily_call_count;default:0" json:"daily_call_count"`
	DailyCallDate      *time.Time `gorm:"column:daily_call_date" json:"daily_call_date"`
	UpdatedAt          time.Time  `gorm:"column:updated_at;autoUpdateTime" json:"updated_at"`
}

func (XeroSyncState) TableName() string { return "xero_sync_state" }

type XeroReportCache struct {
	ID         int       `gorm:"primaryKey;autoIncrement" json:"id"`
	TenantID   string    `gorm:"column:tenant_id;not null;uniqueIndex:idx_xero_report_cache" json:"tenant_id"`
	ReportType string    `gorm:"column:report_type;not null;uniqueIndex:idx_xero_report_cache" json:"report_type"`
	Params     string    `gorm:"column:params;uniqueIndex:idx_xero_report_cache" json:"params"`
	ReportData JSONB     `gorm:"column:report_data;not null" json:"report_data"`
	CachedAt   time.Time `gorm:"column:cached_at;autoCreateTime" json:"cached_at"`
}

func (XeroReportCache) TableName() string { return "xero_reports_cache" }

type ReconciliationOverride struct {
	PlaidID       string    `gorm:"column:plaid_id;primaryKey" json:"plaid_id"`
	VendorName    string    `gorm:"column:vendor_name" json:"vendor_name"`
	Description   string    `gorm:"column:description" json:"description"`
	GLAccountCode string    `gorm:"column:gl_account_code" json:"gl_account_code"`
	Notes         string    `gorm:"column:notes" json:"notes"`
	MatchStatus   string    `gorm:"column:match_status;default:unmatched" json:"match_status"`
	UpdatedAt     time.Time `gorm:"column:updated_at;autoUpdateTime" json:"updated_at"`
	UpdatedBy     string    `gorm:"column:updated_by" json:"updated_by"`
}

func (ReconciliationOverride) TableName() string { return "xero_reconciliation_overrides" }

// ──────────────────────────────────────────────
// Daily Balance Snapshots (Plaid)
// ──────────────────────────────────────────────

type DailyBalanceSnapshot struct {
	ID               int       `gorm:"primaryKey;autoIncrement" json:"id"`
	AccountID        string    `gorm:"column:account_id;not null;uniqueIndex:idx_account_date" json:"account_id"`
	AccountName      string    `gorm:"column:account_name" json:"account_name"`
	InstitutionName  string    `gorm:"column:institution_name" json:"institution_name"`
	AccountType      string    `gorm:"column:account_type" json:"account_type"`
	CurrentBalance   *float64  `gorm:"column:current_balance" json:"current_balance"`
	AvailableBalance *float64  `gorm:"column:available_balance" json:"available_balance"`
	SnapshotDate     string    `gorm:"column:snapshot_date;not null;uniqueIndex:idx_account_date" json:"snapshot_date"`
	Source           string    `gorm:"column:source" json:"source"`
	CreatedAt        time.Time `gorm:"column:created_at;autoCreateTime" json:"created_at"`
}

func (DailyBalanceSnapshot) TableName() string { return "daily_balance_snapshots" }

// ──────────────────────────────────────────────
// Documents (OCR scanning)
// ──────────────────────────────────────────────

type Document struct {
	ID                   int       `gorm:"primaryKey;autoIncrement" json:"id"`
	Filename             string    `gorm:"column:filename;not null" json:"filename"`
	FilePath             string    `gorm:"column:file_path;not null" json:"file_path"`
	FileURL              string    `gorm:"column:file_url" json:"file_url"`
	DocumentType         *string   `gorm:"column:document_type" json:"document_type"`
	VendorName           *string   `gorm:"column:vendor_name" json:"vendor_name"`
	VendorAddress        *string   `gorm:"column:vendor_address" json:"vendor_address"`
	DocumentDate         *string   `gorm:"column:document_date" json:"document_date"`
	DocumentNumber       *string   `gorm:"column:document_number" json:"document_number"`
	TotalAmount          *float64  `gorm:"column:total_amount" json:"total_amount"`
	TaxAmount            *float64  `gorm:"column:tax_amount" json:"tax_amount"`
	LineItems            *string   `gorm:"column:line_items;type:jsonb" json:"line_items"`
	Location             *string   `gorm:"column:location" json:"location"`
	LocationCode         *string   `gorm:"column:location_code" json:"location_code"`
	PONumber             *string   `gorm:"column:po_number" json:"po_number"`
	MatchedTransactionID *string   `gorm:"column:matched_transaction_id" json:"matched_transaction_id"`
	MatchedXeroInvoiceID *int      `gorm:"column:matched_xero_invoice_id" json:"matched_xero_invoice_id"`
	OCRRaw               *string   `gorm:"column:ocr_raw" json:"-"`
	OCRConfidence        *float64  `gorm:"column:ocr_confidence" json:"ocr_confidence"`
	Status               string    `gorm:"column:status;default:pending" json:"status"`
	UploadedBy           *string   `gorm:"column:uploaded_by" json:"uploaded_by"`
	IsDeleted            bool       `gorm:"column:is_deleted;default:false" json:"-"`
	WickedFileSent       bool       `gorm:"column:wickedfile_sent;default:false" json:"wickedfile_sent"`
	WickedFileSentAt     *time.Time `gorm:"column:wickedfile_sent_at" json:"wickedfile_sent_at"`
	CreatedAt            time.Time  `gorm:"column:created_at;autoCreateTime" json:"created_at"`
	UpdatedAt            time.Time  `gorm:"column:updated_at;autoUpdateTime" json:"updated_at"`
}

func (Document) TableName() string { return "documents" }

// ──────────────────────────────────────────────
// Integration Settings (key-value store)
// ──────────────────────────────────────────────

type IntegrationSetting struct {
	ID        int       `gorm:"primaryKey;autoIncrement" json:"id"`
	Key       string    `gorm:"column:key;uniqueIndex;not null" json:"key"`
	Value     string    `gorm:"column:value" json:"value"`
	UpdatedAt time.Time `gorm:"column:updated_at;autoUpdateTime" json:"updated_at"`
}

func (IntegrationSetting) TableName() string { return "integration_settings" }
