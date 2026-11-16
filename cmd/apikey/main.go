// Command apikey provisions and inspects API keys for the job API.
//
// The plaintext key is printed exactly once by "create". Only its SHA-256
// digest is stored, so it cannot be recovered later; revoke it and create a new
// key if a credential is lost.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/auth"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "apikey:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return usageError()
	}

	switch args[0] {
	case "create":
		return runCreate(args[1:])
	case "revoke":
		return runRevoke(args[1:])
	case "list":
		return runList(args[1:])
	case "help", "-h", "--help":
		printUsage()
		return nil
	default:
		return usageError()
	}
}

func runCreate(args []string) error {
	fs := flag.NewFlagSet("create", flag.ContinueOnError)
	tenantArg := fs.String("tenant", "", "tenant UUID")
	nameArg := fs.String("name", "", "unique key name within the tenant")
	roleArg := fs.String("role", string(auth.RoleClient), "client or operator")
	if err := fs.Parse(args); err != nil {
		return err
	}

	tenantID, err := parseTenant(*tenantArg)
	if err != nil {
		return err
	}
	role, err := auth.ParseRole(*roleArg)
	if err != nil {
		return err
	}
	name := strings.TrimSpace(*nameArg)
	if name == "" {
		return errors.New("name is required")
	}

	token, hash, err := auth.GenerateToken()
	if err != nil {
		return err
	}

	pool, ctx, cancel, err := connect()
	if err != nil {
		return err
	}
	defer pool.Close()
	defer cancel()

	key, err := postgres.NewAPIKeyRepository(pool).CreateAPIKey(ctx, tenantID, name, role, hash)
	if err != nil {
		return err
	}

	fmt.Printf("API key created. Store it now; it cannot be retrieved again.\n\n")
	fmt.Printf("  id:      %s\n", key.ID)
	fmt.Printf("  tenant:  %s\n", key.TenantID)
	fmt.Printf("  name:    %s\n", key.Name)
	fmt.Printf("  role:    %s\n", key.Role)
	fmt.Printf("  created: %s\n", key.CreatedAt.UTC().Format(time.RFC3339))
	fmt.Printf("  key:     %s\n", token)
	return nil
}

func runRevoke(args []string) error {
	fs := flag.NewFlagSet("revoke", flag.ContinueOnError)
	tenantArg := fs.String("tenant", "", "tenant UUID")
	nameArg := fs.String("name", "", "key name to revoke")
	if err := fs.Parse(args); err != nil {
		return err
	}

	tenantID, err := parseTenant(*tenantArg)
	if err != nil {
		return err
	}
	name := strings.TrimSpace(*nameArg)
	if name == "" {
		return errors.New("name is required")
	}

	pool, ctx, cancel, err := connect()
	if err != nil {
		return err
	}
	defer pool.Close()
	defer cancel()

	key, err := postgres.NewAPIKeyRepository(pool).RevokeAPIKey(ctx, tenantID, name)
	if err != nil {
		return err
	}

	fmt.Printf("API key revoked.\n\n")
	fmt.Printf("  id:         %s\n", key.ID)
	fmt.Printf("  tenant:     %s\n", key.TenantID)
	fmt.Printf("  name:       %s\n", key.Name)
	fmt.Printf("  role:       %s\n", key.Role)
	if key.RevokedAt != nil {
		fmt.Printf("  revoked_at: %s\n", key.RevokedAt.UTC().Format(time.RFC3339))
	}
	return nil
}

func runList(args []string) error {
	fs := flag.NewFlagSet("list", flag.ContinueOnError)
	tenantArg := fs.String("tenant", "", "tenant UUID")
	if err := fs.Parse(args); err != nil {
		return err
	}

	tenantID, err := parseTenant(*tenantArg)
	if err != nil {
		return err
	}

	pool, ctx, cancel, err := connect()
	if err != nil {
		return err
	}
	defer pool.Close()
	defer cancel()

	keys, err := postgres.NewAPIKeyRepository(pool).ListAPIKeys(ctx, tenantID)
	if err != nil {
		return err
	}

	if len(keys) == 0 {
		fmt.Printf("No API keys for tenant %s.\n", tenantID)
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tNAME\tROLE\tCREATED\tREVOKED")
	for _, key := range keys {
		revoked := "-"
		if key.RevokedAt != nil {
			revoked = key.RevokedAt.UTC().Format(time.RFC3339)
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
			key.ID,
			key.Name,
			key.Role,
			key.CreatedAt.UTC().Format(time.RFC3339),
			revoked,
		)
	}
	return w.Flush()
}

func connect() (*pgxpool.Pool, context.Context, context.CancelFunc, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	pool, err := postgres.Connect(ctx, postgres.Config{
		URL:            strings.TrimSpace(os.Getenv("DATABASE_URL")),
		MaxConns:       2,
		ConnectTimeout: 5 * time.Second,
	})
	if err != nil {
		cancel()
		return nil, nil, nil, fmt.Errorf("connect to PostgreSQL: %w", err)
	}
	return pool, ctx, cancel, nil
}

func parseTenant(raw string) (uuid.UUID, error) {
	tenantID, err := uuid.Parse(strings.TrimSpace(raw))
	if err != nil || tenantID == uuid.Nil {
		return uuid.Nil, errors.New("tenant must be a valid non-zero UUID")
	}
	return tenantID, nil
}

func usageError() error {
	printUsage()
	return errors.New("a subcommand is required")
}

func printUsage() {
	fmt.Fprintln(os.Stderr, `Usage:
  apikey create -tenant <uuid> -name <name> [-role client|operator]
  apikey revoke -tenant <uuid> -name <name>
  apikey list   -tenant <uuid>

DATABASE_URL must point at the migrated job database.`)
}
