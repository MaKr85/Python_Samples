# <snippet_imports>
import os
from azure.core.exceptions import ResourceNotFoundError
from azure.ai.formrecognizer import FormRecognizerClient
from azure.ai.formrecognizer import FormTrainingClient
from azure.core.credentials import AzureKeyCredential
# </snippet_imports>

# <snippet_creds>
endpoint = "https://dp-formrecognizer-mkr-tst.cognitiveservices.azure.com/"
key = "8ea662154e2b476bb45ee681a37bec6b"
# </snippet_creds>

# <snippet_auth>
form_recognizer_client = FormRecognizerClient(endpoint, AzureKeyCredential(key))
form_training_client = FormTrainingClient(endpoint, AzureKeyCredential(key))
# </snippet_auth>



invoiceUrl = "https://dpstoragetstmkr.blob.core.windows.net/98-temporary/FormRecognizer/sampleData/Invoice_1.pdf"

poller = form_recognizer_client.begin_recognize_invoices_from_url(invoiceUrl)
invoices = poller.result()

for idx, invoice in enumerate(invoices):
    print("--------Recognizing invoice #{}--------".format(idx+1))
    vendor_name = invoice.fields.get("VendorName")
    if vendor_name:
        print("Vendor Name: {} has confidence: {}".format(vendor_name.value, vendor_name.confidence))
    vendor_address = invoice.fields.get("VendorAddress")
    if vendor_address:
        print("Vendor Address: {} has confidence: {}".format(vendor_address.value, vendor_address.confidence))
    customer_name = invoice.fields.get("CustomerName")
    if customer_name:
        print("Customer Name: {} has confidence: {}".format(customer_name.value, customer_name.confidence))
    customer_address = invoice.fields.get("CustomerAddress")
    if customer_address:
        print("Customer Address: {} has confidence: {}".format(customer_address.value, customer_address.confidence))
    customer_address_recipient = invoice.fields.get("CustomerAddressRecipient")
    if customer_address_recipient:
        print("Customer Address Recipient: {} has confidence: {}".format(customer_address_recipient.value, customer_address_recipient.confidence))
    invoice_id = invoice.fields.get("InvoiceId")
    if invoice_id:
        print("Invoice Id: {} has confidence: {}".format(invoice_id.value, invoice_id.confidence))
    invoice_date = invoice.fields.get("InvoiceDate")
    if invoice_date:
        print("Invoice Date: {} has confidence: {}".format(invoice_date.value, invoice_date.confidence))
    invoice_total = invoice.fields.get("InvoiceTotal")
    if invoice_total:
        print("Invoice Total: {} has confidence: {}".format(invoice_total.value, invoice_total.confidence))
    due_date = invoice.fields.get("DueDate")
    if due_date:
        print("Due Date: {} has confidence: {}".format(due_date.value, due_date.confidence))

