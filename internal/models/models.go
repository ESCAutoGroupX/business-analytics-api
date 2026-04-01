package models

import (
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
