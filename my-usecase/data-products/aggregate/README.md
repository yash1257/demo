# Aggregate Align Data Product – Setup Guide

This document provides a step-by-step guide to set up and deploy the **Aggregate Align Data Product** using **DataOS**.  
Follow the instructions carefully to ensure smooth configuration and deployment.

---

## Step 1: Locate and Review the Code

1. Navigate to the **`aggregate`** folder.  
2. Inside, you will find a folder named **`bundle`**, which contains the **semantic model**.  
   This folder includes:
   - **SQL file**
   - **Table file**
   - **User groups file**
   - **Deployment YAML**

You can modify these files as per your requirements, but ensure that the **semantic model folder structure remains unchanged**.

---

## Step 2: Review and Modify Data Quality YAML Files

1. Inside the **`bundle`** folder, you will also find **data quality YAML files**.  
2. These files define data validation and quality checks. You can:
   - Modify existing files to adjust quality rules, or  
   - Create new YAML files to add additional checks as needed.

---

## Step 3: Review and Modify the Flare Job

1. Within the **`bundle`** folder, you will find **data ingestion YAML files**, which define **Flare jobs**.  
2. Review and modify these YAML files based on your data ingestion requirements (e.g., source, destination, schedule, and transformations).

---

## Step 4: Update and Deploy the Bundle YAML

1. Open the **`bundle.yaml`** file.  
2. Locate the **`resources`** section — this defines which files will be deployed.  
3. Update the **file paths** in this section to include the correct files you want to deploy.  
4. Once updated, apply the bundle using the command below:

    ```bash
    dataos-ctl apply -f <path-to-your-bundle.yaml>
    ```
After successful deployment, proceed to the next step.

## Step 5: Configure and Deploy the Data Product YAML

Open the **Data Product YAML** file and modify it according to your requirements.

A command section is included in the YAML file to guide you on where to make updates.

Once ready, apply the YAML using:

    ```bash
    dataos-ctl product apply -f <path-to-your-data-product.yaml>
    ```
After the Data Product is successfully deployed, move on to the next step.

## Step 6: Register the Data Product with the Scanner

After deploying the Data Product YAML, locate the Scanner YAML file.

Apply it to register your Data Product on DPH (Data Product Hub) using the command below:

    ```bash
    dataos-ctl apply -f <path-to-your-scanner.yaml>
    ```
Once the scanner is applied, your Data Product will appear in the DataOS UI.

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

Flare Job: https://dataos.info/resources/stacks/flare/

Bundle Structure: https://dataos.info/resources/bundle/#structure-of-a-bundle-yaml-manifest

Data Quality (Soda): https://dataos.info/resources/stacks/soda/

Data Product: https://dataos.info/products/data_product/

Scanner for Data Product: https://dataos.info/resources/stacks/scanner/supported_sources/system_metadata_sources/data_product_scanner/
