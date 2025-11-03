# Source Align Data Product – Setup Guide

This document provides a step-by-step guide to set up and deploy the **Source Align Data Product** using **DataOS**.  
Follow the instructions carefully to ensure smooth configuration and deployment.

---

## Step 1: Locate and Review the Nilus Code

1. Navigate to the **`source`** folder.  
2. Inside, you will find a folder named **`realtime-api-code`**, which contains:
   - **Custom Python code** for the Nilus component.  
   - **Deployment YAML files** — one for **Imperial units** and another for **Metric units**.  

Review and modify these files based on your deployment requirements.

---

## Step 2: Review and Modify Quality YAML Files

1. Inside the **`bundle`** folder, you will also find **data quality YAML files**.  
2. You can modify these files to:
   - Add additional data quality checks, or  
   - Create new ones as per your project requirements.  

For detailed guidance, refer to the [Data Quality (Soda) documentation](https://dataos.info/resources/stacks/soda/).

---

## Step 3: Update and Deploy the Bundle YAML

1. Open the **`bundle.yaml`** file.  

2. Locate the **`resources`** section, this defines which files will be deployed.

3. Update the **file paths** in this section to include the correct files you wish to deploy.

4. Once the paths are updated, apply the bundle using the following command:

   ```bash
   dataos-ctl apply -f <path-to-your-bundle.yaml>
   ```
After successful deployment, proceed to the next step.

## Step 4: Configure and Deploy the Data Product YAML

1. Open the Data Product YAML file and update it as per your requirements.

2. A command section is included within the YAML file to guide you on where and what to modify.

3. Once ready, apply the YAML file using the following command:

   ```bash
   dataos-ctl product apply -f <path-to-your-data-product.yaml>
   ```
After the Data Product is successfully deployed, move to the next step.

## Step 5: Register the Data Product with the Scanner

After deploying the Data Product YAML, locate the Scanner YAML file.

Apply the file using the following command to register the Data Product on DPH (Data Product Hub):

   ```bash
   dataos-ctl apply -f <path-to-your-scanner.yaml>
   ```
Once the scanner is applied, your Data Product will be visible on the DataOS UI.

## Required Environment Variables and Configurations

Ensure that the following configuration values are included in your deployment YAML files:

   ```yaml
   workspace: public             # your workspace name
   compute: cola-release
   name: gitrepocred-r           # Git secret name
   name: weather-api-cred        # Instance secret name created for the Weather API
   clustername: <your-cluster-name>  # Replace with your actual cluster name
   ```

## Reference Links

Nilus Code: https://dataos.info/resources/stacks/nilus/

Bundle Structure: https://dataos.info/resources/bundle/#structure-of-a-bundle-yaml-manifest

Data Quality (Soda): https://dataos.info/resources/stacks/soda/

Data Product: https://dataos.info/products/data_product/

Scanner for Data Product: https://dataos.info/resources/stacks/scanner/supported_sources/system_metadata_sources/data_product_scanner/


