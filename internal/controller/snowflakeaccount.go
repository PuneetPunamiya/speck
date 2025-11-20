package controller

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"

	operatorv1alpha1 "github.com/redhat-data-and-ai/speck/api/v1alpha1"
	_ "github.com/snowflakedb/gosnowflake"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// snowflakeCredentials holds the credentials needed to connect to Snowflake
type snowflakeCredentials struct {
	username string
	password string
	account  string
	role     string
}

// accountDetails holds the details of a created Snowflake account
type accountDetails struct {
	accountName   string
	adminName     string
	adminPassword string
	email         string
	region        string
	edition       string
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
// Returns the account details and any error
func (r *SnowflakeAccountReconciler) createSnowflakeAccount(ctx context.Context, account *operatorv1alpha1.SnowflakeAccount) (*accountDetails, error) {
	log := logf.FromContext(ctx)

	// Get Snowflake credentials from environment variables
	creds, err := getSnowflakeCredentialsFromEnv()
	if err != nil {
		return nil, err
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

	// Log account creation (without sensitive credentials)
	log.Info("Creating Snowflake account",
		"accountName", accountName,
		"region", region,
		"edition", edition,
		"resourceName", account.Name,
		"namespace", account.Namespace)

	// Connect to Snowflake
	db, err := connectToSnowflake(creds)
	if err != nil {
		return nil, err
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
		return nil, fmt.Errorf("failed to execute CREATE ACCOUNT: %w", err)
	}

	log.Info("Snowflake account created successfully", "accountName", accountName)

	// Return account details for secret creation
	return &accountDetails{
		accountName:   accountName,
		adminName:     adminName,
		adminPassword: adminPassword,
		email:         email,
		region:        region,
		edition:       edition,
	}, nil
}

// createCredentialsSecret creates a Kubernetes Secret to store the Snowflake account credentials
func (r *SnowflakeAccountReconciler) createCredentialsSecret(ctx context.Context, account *operatorv1alpha1.SnowflakeAccount, details *accountDetails) error {
	log := logf.FromContext(ctx)

	// Create secret name: {accountName}-creds (lowercase for Kubernetes naming requirements)
	secretName := fmt.Sprintf("%s-creds", strings.ToLower(details.accountName))

	// Prepare secret data
	secretData := map[string][]byte{
		"accountName":   []byte(details.accountName),
		"adminName":     []byte(details.adminName),
		"adminPassword": []byte(details.adminPassword),
		"email":         []byte(details.email),
		"region":        []byte(details.region),
		"edition":       []byte(details.edition),
		"accountURL":    []byte(fmt.Sprintf("https://%s.snowflakecomputing.com", details.accountName)),
	}

	// Create the Secret object
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: account.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":       "snowflake-account",
				"app.kubernetes.io/managed-by": "snowflake-operator",
				"app.kubernetes.io/instance":   account.Name,
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: account.APIVersion,
					Kind:       account.Kind,
					Name:       account.Name,
					UID:        account.UID,
					Controller: boolPtr(true),
				},
			},
		},
		Type: corev1.SecretTypeOpaque,
		Data: secretData,
	}

	// Create the secret in the cluster
	if err := r.Create(ctx, secret); err != nil {
		log.Error(err, "Failed to create credentials secret", "secretName", secretName)
		return fmt.Errorf("failed to create secret: %w", err)
	}

	log.Info("Successfully created credentials secret", "secretName", secretName, "namespace", account.Namespace)
	return nil
}

// boolPtr returns a pointer to a bool value
func boolPtr(b bool) *bool {
	return &b
}

// deleteSnowflakeAccount deletes a Snowflake account using the DROP ACCOUNT command
// Returns any error encountered during deletion
func (r *SnowflakeAccountReconciler) deleteSnowflakeAccount(ctx context.Context, account *operatorv1alpha1.SnowflakeAccount) error {
	log := logf.FromContext(ctx)

	// Extract the account name from the status or from the secret
	accountName := extractAccountNameFromURL(account.Status.AccountURL)
	if accountName == "" {
		// Try to get it from the secret
		accountName, err := r.getAccountNameFromSecret(ctx, account)
		if err != nil {
			log.Error(err, "Failed to get account name from secret")
			log.Info("No account name found, skipping deletion")
			return nil
		}
		if accountName == "" {
			log.Info("No account name found in status or secret, skipping deletion")
			return nil
		}
	}

	// Get Snowflake organization credentials from environment variables
	creds, err := getSnowflakeCredentialsFromEnv()
	if err != nil {
		return err
	}

	log.Info("Deleting Snowflake account",
		"accountName", accountName,
		"orgAccount", creds.account,
		"orgRole", creds.role)

	// Connect to Snowflake
	db, err := connectToSnowflake(creds)
	if err != nil {
		return err
	}
	defer db.Close()

	// Set a timeout for the operation
	deleteCtx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()

	// Build DROP ACCOUNT SQL with IF EXISTS and GRACE_PERIOD_IN_DAYS
	// Using 3 days grace period by default
	dropAccountSQL := fmt.Sprintf(`DROP ACCOUNT IF EXISTS %s GRACE_PERIOD_IN_DAYS = 3`, accountName)

	log.Info("Executing DROP ACCOUNT", "sql", dropAccountSQL)

	// Execute the DROP ACCOUNT statement
	_, err = db.ExecContext(deleteCtx, dropAccountSQL)
	if err != nil {
		return fmt.Errorf("failed to execute DROP ACCOUNT: %w", err)
	}

	log.Info("Successfully executed DROP ACCOUNT", "accountName", accountName)
	return nil
}

// getAccountNameFromSecret retrieves the account name from the credentials secret
func (r *SnowflakeAccountReconciler) getAccountNameFromSecret(ctx context.Context, account *operatorv1alpha1.SnowflakeAccount) (string, error) {
	log := logf.FromContext(ctx)

	// List secrets in the namespace with our label
	secretList := &corev1.SecretList{}
	listOpts := []client.ListOption{
		client.InNamespace(account.Namespace),
		client.MatchingLabels{
			"app.kubernetes.io/instance": account.Name,
		},
	}

	if err := r.List(ctx, secretList, listOpts...); err != nil {
		return "", fmt.Errorf("failed to list secrets: %w", err)
	}

	if len(secretList.Items) == 0 {
		log.Info("No credential secret found for account")
		return "", nil
	}

	// Get the account name from the first matching secret
	secret := secretList.Items[0]
	accountName := string(secret.Data["accountName"])

	log.Info("Found account name from secret", "secretName", secret.Name, "accountName", accountName)
	return accountName, nil
}
