# interlink-kueue-plugin
A Container plugin (aka sidecar) to connect kueue to interlink

## Install in a kubernetes cluster

You can install the interlink endpoint in a kubernetes cluster using the helm chart in this repository.

Here is the minimal `values.yaml` to configure it.

```yaml
# Deployment hostname with full domain, but without protocol
hostname: ## Insert here the hostname of the deployment, for example `123.45.67.89.myip.cloud.infn.it`
 
# certManagerEnabled if true, provides a certmanager for TLS termination
# IMPORTANT!!! Disable it if installing the chart in a already certified cluster
certManagerEnabled: true

# certManagerEmailAddress is the email address of the certificate owner
certManagerEmailAddress:  ## Insert your email here
 
# Indigo IAM Client configuration
iamClientId:  ## Insert the IAM Client ID here
iamClientSecret:  ## Insert the IAM Client secret here

### Note: make sure the client expects the callback from `https://<your hostname>/interlink/callback`

iamAllowedGroup: ## Insert here the IAM group allowed to access the interLink endpoint

oauth2ProxyCookieSecret:  ## Insert here a random string
 
jobsNamespace: ## Choose a name for the namespace where the jobs will be executed

## Define the quota for cpu, memory and gpus provisioned via this interlink
totalCpuQuota: 0
totalMemoryQuota: 0
totalGpuQuota: 0

```
