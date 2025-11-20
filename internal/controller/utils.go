package controller

import (
	"context"
	"crypto/rand"
	"fmt"
	"math/big"

	operatorv1alpha1 "github.com/redhat-data-and-ai/speck/api/v1alpha1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// updateStatusAfterCreation updates the SnowflakeAccount status after successful account creation
func (r *SnowflakeAccountReconciler) updateStatusAfterCreation(ctx context.Context, snowflakeAccount *operatorv1alpha1.SnowflakeAccount, details *accountDetails) error {
	log := logf.FromContext(ctx)

	// Update status fields
	snowflakeAccount.Status.AccountCreated = true
	snowflakeAccount.Status.AccountURL = fmt.Sprintf("https://%s.snowflakecomputing.com", details.accountName)
	snowflakeAccount.Status.Message = "Snowflake account created successfully"

	// Persist the status update
	if err := r.Status().Update(ctx, snowflakeAccount); err != nil {
		log.Error(err, "Failed to update status after account creation")
		return err
	}

	return nil
}

// generateRandomAccountName generates a random account name (8 uppercase alphanumeric characters)
func generateRandomAccountName() string {
	return "SF" + generateRandomString(6, "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
}

// generateRandomUsername generates a random username
func generateRandomUsername() string {
	return "admin_" + generateRandomString(8, "abcdefghijklmnopqrstuvwxyz0123456789")
}

// generateRandomPassword generates a secure random password
func generateRandomPassword() string {
	// Password with uppercase, lowercase, numbers, and special characters
	upper := generateRandomString(4, "ABCDEFGHIJKLMNOPQRSTUVWXYZ")
	lower := generateRandomString(4, "abcdefghijklmnopqrstuvwxyz")
	numbers := generateRandomString(4, "0123456789")
	special := generateRandomString(2, "!@#$%^&*")

	// Combine and shuffle
	password := upper + lower + numbers + special
	return shuffleString(password)
}

// generateRandomString generates a random string of specified length from the given charset
func generateRandomString(length int, charset string) string {
	result := make([]byte, length)
	charsetLen := big.NewInt(int64(len(charset)))

	for i := 0; i < length; i++ {
		num, err := rand.Int(rand.Reader, charsetLen)
		if err != nil {
			// Fallback to a simple character if random fails
			result[i] = charset[i%len(charset)]
			continue
		}
		result[i] = charset[num.Int64()]
	}

	return string(result)
}

// shuffleString shuffles the characters in a string
func shuffleString(s string) string {
	runes := []rune(s)
	n := len(runes)

	for i := n - 1; i > 0; i-- {
		jBig, err := rand.Int(rand.Reader, big.NewInt(int64(i+1)))
		if err != nil {
			continue
		}
		j := int(jBig.Int64())
		runes[i], runes[j] = runes[j], runes[i]
	}

	return string(runes)
}
