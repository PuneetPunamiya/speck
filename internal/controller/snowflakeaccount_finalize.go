package controller

import (
	"context"
	"fmt"

	operatorv1alpha1 "github.com/redhat-data-and-ai/speck/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	// snowflakeAccountFinalizer is the finalizer name for SnowflakeAccount
	snowflakeAccountFinalizer = "operator.dataverse.redhat.com/finalizer"
)

func (r *SnowflakeAccountReconciler) handleFinalizerOperations(ctx context.Context, snowflakeAccount *operatorv1alpha1.SnowflakeAccount) (continueReconciliation bool, err error) {
	log := logf.FromContext(ctx)

	// Check if the SnowflakeAccount is being deleted
	if !snowflakeAccount.DeletionTimestamp.IsZero() {
		// The object is being deleted
		if controllerutil.ContainsFinalizer(snowflakeAccount, snowflakeAccountFinalizer) {
			log.Info("Running finalizer logic for SnowflakeAccount")

			// Perform cleanup operations
			if err := r.finalizeSnowflakeAccount(ctx, snowflakeAccount); err != nil {
				log.Error(err, "Failed to finalize SnowflakeAccount")
				return false, err
			}

			// Remove the finalizer
			controllerutil.RemoveFinalizer(snowflakeAccount, snowflakeAccountFinalizer)
			if err := r.Update(ctx, snowflakeAccount); err != nil {
				log.Error(err, "Failed to remove finalizer")
				return false, err
			}
			log.Info("Successfully finalized SnowflakeAccount")
		}
		return false, nil
	}

	// Add finalizer if it doesn't exist
	if !controllerutil.ContainsFinalizer(snowflakeAccount, snowflakeAccountFinalizer) {
		log.Info("Adding finalizer to SnowflakeAccount")
		controllerutil.AddFinalizer(snowflakeAccount, snowflakeAccountFinalizer)
		if err := r.Update(ctx, snowflakeAccount); err != nil {
			log.Error(err, "Failed to add finalizer")
			return false, err
		}
		return false, nil
	}

	// Continue with normal reconciliation
	return true, nil
}

// finalizeSnowflakeAccount performs cleanup operations before the SnowflakeAccount is deleted
func (r *SnowflakeAccountReconciler) finalizeSnowflakeAccount(ctx context.Context, snowflakeAccount *operatorv1alpha1.SnowflakeAccount) error {
	log := logf.FromContext(ctx)
	log.Info("Finalizing SnowflakeAccount", "name", snowflakeAccount.Name, "namespace", snowflakeAccount.Namespace)

	// If the account was created, delete it from Snowflake
	if snowflakeAccount.Status.AccountCreated {
		log.Info("Deleting Snowflake account", "accountURL", snowflakeAccount.Status.AccountURL)

		if err := r.deleteSnowflakeAccount(ctx, snowflakeAccount); err != nil {
			log.Error(err, "Failed to delete Snowflake account, will retry")
			return fmt.Errorf("failed to delete Snowflake account: %w", err)
		}

		log.Info("Successfully deleted Snowflake account")
	} else {
		log.Info("Snowflake account was not created, skipping deletion")
	}

	log.Info("Successfully finalized SnowflakeAccount")
	return nil
}
