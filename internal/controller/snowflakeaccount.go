package controller

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	operatorv1alpha1 "github.com/redhat-data-and-ai/speck/api/v1alpha1"
	_ "github.com/snowflakedb/gosnowflake"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// snowflakeCredentials holds the credentials needed to connect to Snowflake
type snowflakeCredentials struct {
	username string
	password string
	account  string
	role     string
}

// getSnowflakeCredentialsFromEnv fetches and validates organization credentials from environment variables
func getSnowflakeCredentialsFromEnv() (*snowflakeCredentials, error) {
	// Read credentials from environment variables
	orgUsername := os.Getenv("SNOWFLAKE_ORG_USERNAME")
	orgPassword := os.Getenv("SNOWFLAKE_ORG_PASSWORD")
	orgAccount := os.Getenv("SNOWFLAKE_ORG_ACCOUNT")
	orgRole := os.Getenv("SNOWFLAKE_ORG_ROLE")

	// Validate required fields
	if orgUsername == "" {
		return nil, fmt.Errorf("environment variable SNOWFLAKE_ORG_USERNAME is required but not set")
	}
	if orgPassword == "" {
		return nil, fmt.Errorf("environment variable SNOWFLAKE_ORG_PASSWORD is required but not set")
	}
	if orgAccount == "" {
		return nil, fmt.Errorf("environment variable SNOWFLAKE_ORG_ACCOUNT is required but not set")
	}

	// Default role if not specified
	if orgRole == "" {
		orgRole = "ORGADMIN"
	}

	return &snowflakeCredentials{
		username: orgUsername,
		password: orgPassword,
		account:  orgAccount,
		role:     orgRole,
	}, nil
}

// connectToSnowflake establishes a connection to Snowflake using the provided credentials
func connectToSnowflake(creds *snowflakeCredentials) (*sql.DB, error) {
	// Build the DSN (Data Source Name)
	// Format: username:password@account?role=ORGADMIN
	dsn := fmt.Sprintf("%s:%s@%s?role=%s",
		creds.username,
		creds.password,
		creds.account,
		creds.role)

	// Open connection to Snowflake
	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection: %w", err)
	}

	return db, nil
}

// createSnowflakeAccount creates a new Snowflake account
// Returns the generated account name and any error
func (r *SnowflakeAccountReconciler) createSnowflakeAccount(ctx context.Context, account *operatorv1alpha1.SnowflakeAccount) (string, error) {
	log := logf.FromContext(ctx)

	// Get Snowflake credentials from environment variables
	creds, err := getSnowflakeCredentialsFromEnv()
	if err != nil {
		return "", err
	}

	// Generate all account details
	accountName := generateRandomAccountName()
	adminName := generateRandomUsername()
	adminPassword := generateRandomPassword()
	firstName := "Admin"
	lastName := "User"
	email := fmt.Sprintf("%s@example.com", adminName) // Generate email from admin name
	region := "AWS_US_WEST_2"
	edition := "ENTERPRISE"
	comment := "Created by Kubernetes Operator"

	// Log all generated credentials for reference
	log.Info("Creating Snowflake account with generated credentials",
		"accountName", accountName,
		"adminName", adminName,
		"adminPassword", adminPassword,
		"firstName", firstName,
		"lastName", lastName,
		"email", email,
		"region", region,
		"edition", edition,
		"orgAccount", creds.account,
		"orgRole", creds.role,
		"resourceName", account.Name,
		"namespace", account.Namespace)

	// Connect to Snowflake
	db, err := connectToSnowflake(creds)
	if err != nil {
		return "", err
	}
	defer db.Close()

	// Set a timeout for the operation
	createCtx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()

	// Build CREATE ACCOUNT SQL
	createAccountSQL := fmt.Sprintf(`
        CREATE ACCOUNT %s
            ADMIN_NAME = '%s'
            ADMIN_PASSWORD = '%s'
            ADMIN_USER_TYPE = PERSON
            FIRST_NAME = '%s'
            LAST_NAME = '%s'
            EMAIL = '%s'
            MUST_CHANGE_PASSWORD = TRUE
            EDITION = %s
            REGION = '%s'
            COMMENT = '%s'
    `,
		accountName,
		adminName,
		adminPassword,
		firstName,
		lastName,
		email,
		edition,
		region,
		comment)

	log.Info("Executing CREATE ACCOUNT SQL")

	// Execute the CREATE ACCOUNT statement
	_, err = db.ExecContext(createCtx, createAccountSQL)
	if err != nil {
		return "", fmt.Errorf("failed to execute CREATE ACCOUNT: %w", err)
	}

	log.Info("Snowflake account created successfully", "accountName", accountName)
	return accountName, nil
}
