# HubSpot Data Loader & Snowflake Sync

Automatically populate your HubSpot account with realistic sample data and sync it to Snowflake using GitHub Actions.

## Features

- 🔄 **Incremental Loading**: Automatically adds new sample data to HubSpot
- ❄️ **Snowflake Sync**: Syncs HubSpot data to Snowflake with incremental updates
- ✅ **Data Validation**: Ensures source and destination have matching record counts
- 🎯 **Realistic Data**: Uses Faker library to generate believable contacts and companies
- ⚙️ **Configurable**: Control how many contacts/companies to create
- 🚀 **Automated**: GitHub Actions handles everything
- 🔒 **Secure**: All credentials stored as GitHub secrets
- 🛡️ **Rate Limiting**: Robust exponential backoff retry logic handles API rate limits gracefully

## Setup Instructions

### 1. Create GitHub Repository

1. Go to [GitHub](https://github.com/new)
2. Create a new **private** repository named `hubspot-data-loader`
3. Don't initialize with README (we'll push our code)

### 2. Add GitHub Secrets

Go to your repository Settings → Secrets and variables → Actions, and add the following secrets:

#### HubSpot Credentials
- **Name**: `HUBSPOT_API_KEY`
- **Value**: Your HubSpot Private App access token (starts with `pat-eu1-...`)

**To get your HubSpot API key:**
- Go to HubSpot → Settings → Integrations → Private Apps
- Create a private app with scopes: `crm.objects.contacts.read`, `crm.objects.contacts.write`, `crm.objects.companies.read`, `crm.objects.companies.write`
- Copy the access token

#### Snowflake Credentials
Add these secrets for the Snowflake sync:

- **Name**: `SNOWFLAKE_ACCOUNT`
- **Value**: Your Snowflake account identifier (e.g., `ORGNAME-ACCOUNT123`)

- **Name**: `SNOWFLAKE_USER`
- **Value**: Your Snowflake username (e.g., `YOUR_USERNAME`)

- **Name**: `SNOWFLAKE_PASSWORD`
- **Value**: Your Snowflake password

- **Name**: `SNOWFLAKE_WAREHOUSE`
- **Value**: Your Snowflake warehouse name (e.g., `COMPUTE_WH`)

- **Name**: `SNOWFLAKE_DATABASE`
- **Value**: Database name for storing HubSpot data (e.g., `HUBSPOT_DATA`)

- **Name**: `SNOWFLAKE_SCHEMA` (Optional)
- **Value**: Schema name (defaults to `PUBLIC` if not set)

### 3. Push Code to GitHub

```bash
cd /Users/ifeoluwaakande/Downloads/hubspot-data-loader
git init
git add .
git commit -m "Initial commit: HubSpot data loader"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/hubspot-data-loader.git
git push -u origin main
```

### 4. Run the Workflows

#### Load HubSpot Sample Data
**Option A: Manual Run (Test it now)**
1. Go to your repo → Actions tab
2. Click "Load HubSpot Sample Data" workflow
3. Click "Run workflow"
4. Set number of contacts and companies
5. Click "Run workflow"

**Option B: Automatic Runs**
- The workflow runs automatically every 5 minutes
- Adds 20 contacts and 10 companies each time
- Perfect for incremental data loading

#### Sync HubSpot Data to Snowflake
**Option A: Manual Run**
1. Go to your repo → Actions tab
2. Click "Sync HubSpot Data to Snowflake" workflow
3. Click "Run workflow"

**Option B: Automatic Runs**
- The workflow runs automatically every 15 minutes
- Syncs all new and updated contacts/companies to Snowflake
- Validates that record counts match between source and destination

## Local Testing

Create a `.env` file with your credentials and test the scripts locally:

```bash
# Install dependencies
pip3 install -r requirements.txt

# Test HubSpot data loading
python3 load_data.py

# Test Snowflake sync
python3 sync_to_snowflake.py
```

**Example `.env` file:**
```bash
# HubSpot Configuration
HUBSPOT_API_KEY=pat-eu1-xxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
NUM_CONTACTS=20
NUM_COMPANIES=10

# Snowflake Configuration
SNOWFLAKE_ACCOUNT=YOUR_ORG-YOUR_ACCOUNT
SNOWFLAKE_USER=YOUR_USERNAME
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=HUBSPOT_DATA
SNOWFLAKE_SCHEMA=PUBLIC
```

Then run with environment variables:
```bash
export $(cat .env | xargs) && python3 sync_to_snowflake.py
```

## Configuration

Edit the workflow file to change:
- **Schedule**: Modify the cron expression in `.github/workflows/load-data.yml`
- **Default Amounts**: Change `NUM_CONTACTS` and `NUM_COMPANIES` defaults

## What Data Gets Created

### Companies (HubSpot & Snowflake)
- Company name
- Domain
- Industry (Technology, Healthcare, Finance, etc.)
- City and Country
- HubSpot timestamps (create and last modified dates)

### Contacts (HubSpot & Snowflake)
- First and Last name
- Email address
- Phone number
- Job title (Sales Manager, CEO, etc.)
- Associated company
- HubSpot timestamps (create and last modified dates)

### Snowflake Tables
The sync creates three tables in Snowflake:

1. **CONTACTS** - All contact records from HubSpot
2. **COMPANIES** - All company records from HubSpot
3. **SYNC_METADATA** - Tracks sync status and validates data consistency

## View Your Data

After running, view your data in HubSpot:
- **Contacts**: https://app-eu1.hubspot.com/contacts/147357939/objects/0-1/views/all/list
- **Companies**: https://app-eu1.hubspot.com/contacts/147357939/objects/0-2/views/all/list

## Troubleshooting

- **409 Errors**: Contact/company already exists (script skips duplicates automatically)
- **401 Errors**: Check your API key in GitHub secrets
- **Missing Scopes**: Verify API key has `crm.objects.contacts.read`, `crm.objects.contacts.write`, `crm.objects.companies.read`, and `crm.objects.companies.write` scopes
- **429 Rate Limit Errors**: Scripts automatically handle rate limits with exponential backoff and retry (up to 5 attempts)
- **Snowflake Connection Issues**: Verify account identifier, credentials, and warehouse are correct in GitHub secrets

## Performance & Scalability

### Rate Limiting
Both `load_data.py` and `sync_to_snowflake.py` include robust rate limiting:
- **Automatic Retry**: Up to 5 retry attempts with exponential backoff
- **429 Handling**: Respects HubSpot's `Retry-After` header
- **Smart Delays**: 100ms delays between pagination pages to avoid rate limits
- **Server Error Handling**: Automatically retries on 5xx errors

### Data Scale
The pipeline can handle:
- **Thousands of records**: Pagination handles large datasets efficiently
- **Frequent Updates**: Incremental loading only syncs changed records
- **Batch Processing**: Snowflake uses staging tables for optimal performance

## Notes

- Duplicate emails are automatically skipped
- Safe to run multiple times (idempotent operations)
- All data is realistic but fake (generated by Faker library)
- Incremental sync tracks timestamps to minimize API calls
- Record counts are validated after each sync

