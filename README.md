# Python Project - Amazon Listing Files:

![](https://github.com/RickyMehra06/Amazon_Listing_Files_AWS/blob/main/media/AmazonListingFiles.gif)


# Problem Statement:
Amazon US facilitates Indian sellers to sell their products in the US marketplace through their seller-fulfilled channels using a program called Amazon Global Selling. An Amazon seller in India is selling his books at Amazon's US marketplace.

The seller wants to create bulk listing excel files to be uploaded at Amazon seller portal. 
The inventory files are received in the form of excel/csv from the publisher or distributors on daily basis.

Using MS Excel it takes 40-45 minutes to preprocess each invetory file in order to create the required bulk listing file for a particular region. Along with performing the preprocessing steps, one of the major challenge to achieve the target selling price (SP) such that the seller wants a fixed amount of profit in this highly competative business environment.

The seller has neary 75,000 as inventory records from the multiple stackholders, if the seller seeks helps from Amazon.com or its service partners to create the bulk listing files for such amount of inventory on their behalf, it will be charged nearly INR 1 for each inventory means the manager has to pay 75,000x3 = 225,000 INR for all three target countries The USA, Canada and Australia.

The seller wants to automate the whole process using Python in order to save both time and money.

## Challenges and other objectives:

* Using MS Excel it takes 40-45 minutes to preprocess the each invetory file in order to create the required bulk listing file for a particular region.
* Since Amazon fee is charged on selling price, using MS excel seller cannot achieved the target selling price for the desired amount of profit.


## Solution Provided:

* Time taken to complete the task is nearly 8 minutes at its extream.
* Gradient descent approach is used to achieve the target selling price for the desired amount of profit.
* Flask API is used to take the user inputs like daily currency exchange rate, target profit, target minimum profit.



### Features in Inventory file:

* ISBN: Internation standard book number
* Name: Name of the product
* Currency: The product can be purchased in which currency
* Price: MRP of the product
* Quantity: Quantity available for the respective product
* Publisher: Name of the Publisher/Distributor
* Date: Inventory date

### Features in Master_File:
* ISBN: Internation standard book number
* ASIN10: Amazon Standard Identification Number
* Title: Name of the product
* Weight: Weight of the product in KG
* Status: Product status regarding listing
* Disclaimer: Disclaimer status


 ### 1. Fillz Listing File:
 * SKU: User defined unique number of the product required to identify whose order has been received.
 * Product-id: It contain ASIN by which particular product is listed at Amazon seller portal.
 * Product-id-type: 
 * Price: Selling price of the product. It is calculated by considering the cost price, packing charge, shipping charge and Amazon fee.
 * Item-condition:
 * Quantity: Quantity of the product to be uploaded at Amazon portal.
 * Add-delete: 'a' is an alias to add a product.
 * Will-ship-internationally: Used to ship internationally from the domestics location.

 ### 2. Automated Price File:
 
 * SKU: User defined unique number of the product required to identify whose order has been received.
 * Minimum-seller-allowed-price: Minimum price below the buy-box price.
 * Maximum-seller-allowed-price: Maximum price above the buy-box price.
 * Country-code: Two letters country code of the target country.
 * Currency-code: Three letters currency code for the target country.
 * Rule-name: User defined Rule Name given at Amazon portal.
 * Rule-action: 'start' is used to to activate the automate pricing rule as per the given rule name.


