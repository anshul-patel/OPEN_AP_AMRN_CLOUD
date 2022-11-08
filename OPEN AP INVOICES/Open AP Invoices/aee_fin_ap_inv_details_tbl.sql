create table xxap.aee_fin_ap_inv_details_tbl
(
invoice_id number,
invoice_num varchar2(50),
invoice_line_number number,
distribution_line_number number,
invoice_type_lookup_code varchar2(25),
line_type_lookup_code varchar2(25),
invoice_date date,
source varchar2(25),
payment_status_flag varchar2(1),
invoice_amount number,
cancelled_amount number,
amount number,
quantity_invoiced number,
unit_price number,
header_description varchar2(240),
line_description varchar2(240),
ship_to_location_code varchar2(60),
vendor_num varchar2(230),
vendor_name varchar2(240),
vendor_site_code varchar2(15),
pay_group_lookup_code varchar2(25),
legal_entity_name varchar2(25),
terms_name varchar2(50), 
payment_method_code varchar2(30),
voucher_num varchar2(50),
requester_first_name varchar2(150),
requester_last_name varchar2(150),
requester_employee_num varchar2(30),
po_number varchar2(50),
po_line_number number ,
po_shipment_num number,
po_distribution_num number,
match_status_flag varchar2(1),
invoice_type varchar2(100)
);

create index xxap.aee_fin_ap_inv_details_tbl_idx1 on xxap.aee_fin_ap_inv_details_tbl_tbl (invoice_id);
create index xxap.aee_fin_ap_inv_details_tbl_idx2 on xxap.aee_fin_ap_inv_details_tbl_tbl (invoice_line_number);
create index xxap.aee_fin_ap_inv_details_tbl_idx3 on xxap.aee_fin_ap_inv_details_tbl_tbl (distribution_line_number);
create index xxap.aee_fin_ap_inv_details_tbl_idx4 on xxap.aee_fin_ap_inv_details_tbl_tbl (line_type_lookup_code);
create index xxap.aee_fin_ap_inv_details_tbl_idx5 on xxap.aee_fin_ap_inv_details_tbl_tbl (payment_status_flag);
create index xxap.aee_fin_ap_inv_details_tbl_idx6 on xxap.aee_fin_ap_inv_details_tbl_tbl (invoice_amount);

create or replace synonym apps.aee_fin_ap_inv_details_tbl for xxap.aee_fin_ap_inv_details_tbl;