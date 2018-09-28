package com.es.pc.esexample.service;

import com.es.pc.esexample.manager.ElasticSearchFilingManager;
import com.es.pc.esexample.utils.FilingUtils;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.ByteOrderMark;
import org.apache.commons.io.input.BOMInputStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@Api(basePath = "/api/v1/filings", value = "Filing Service",
        description = "Retrieval of Form 5500 Filings",
        produces = MediaType.APPLICATION_JSON_VALUE)
@RestController
@RequestMapping("/api/v1/filings")
public class FilingService {

    private static Logger logger = Logger.getLogger(FilingService.class.getName());

    @Autowired
    private ElasticSearchFilingManager elasticSearchFilingManager;

    @ApiOperation(value = "Find filings by plan name", response = List.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved filings"),
            @ApiResponse(code = 500, message = "Failed to retrieve filings"),
    })
    @RequestMapping(value = "/filing/plan/{planName}", method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<List<Map<String, Object>>> findFilingsByPlanName(@PathVariable("planName") String planName) {
        try {
            List<Map<String, Object>> filings = elasticSearchFilingManager.findFilingsByPlanName(planName);
            return new ResponseEntity<>(filings, HttpStatus.OK);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error occurred while fetching filings");
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @ApiOperation(value = "Find filings by sponsor name", response = List.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved filings"),
            @ApiResponse(code = 500, message = "Failed to retrieve filings"),
    })
    @RequestMapping(value = "/filing/sponsor-name/{sponsorName}", method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<List<Map<String, Object>>> findFilingsBySponsorName(@PathVariable("sponsorName") String sponsorName) {
        try {
            List<Map<String, Object>> filings = elasticSearchFilingManager.findFilingsBySponsorName(sponsorName);
            return new ResponseEntity<>(filings, HttpStatus.OK);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error occurred while fetching filings");
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @ApiOperation(value = "Find filings by sponsor state", response = List.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved filings"),
            @ApiResponse(code = 500, message = "Failed to retrieve filings"),
    })
    @RequestMapping(value = "/filing/sponsor-state/{sponsorState}", method = RequestMethod.GET,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<List<Map<String, Object>>> findFilingsBySponsorState(@PathVariable("sponsorState") String sponsorState) {
        try {
            List<Map<String, Object>> filings = elasticSearchFilingManager.findFilingsBySponsorState(sponsorState);
            return new ResponseEntity<>(filings, HttpStatus.OK);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error occurred while fetching filings");
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @ApiOperation(value = "Upload Filing Data")
    @ApiResponses(value = {
            @ApiResponse(code = 201, message = "Successfully uploaded data"),
            @ApiResponse(code = 500, message = "Failed to upload data"),
    })
    @RequestMapping(value="/upload", method=RequestMethod.POST)
    public void uploadFile(@RequestParam MultipartFile file) throws IOException {
//        BufferedReader br;
//        List<String> result = new ArrayList<>();
//        try {
//
//            String line;
//            InputStream is = file.getInputStream();
//            br = new BufferedReader(new InputStreamReader(is));
//            while ((line = br.readLine()) != null) {
//                result.add(line);
//            }
//        } catch (IOException e) {
//            System.err.println(e.getMessage());
//        }
//        elasticSearchFilingManager.processCsv(result);

        BOMInputStream bOMInputStream = new BOMInputStream(file.getInputStream());
        ByteOrderMark bom = bOMInputStream.getBOM();
        String charsetName = bom == null ? "UTF-8" : bom.getCharsetName();
        InputStreamReader reader = new InputStreamReader(new BufferedInputStream(bOMInputStream), charsetName);
        Iterable<CSVRecord> records = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(reader);
        List<Map<String, String>> recordList = new ArrayList<>();
        for (CSVRecord csvRecord : records) {
            Map<String, String> recordMap = new HashMap<>();
            recordMap.put("ACK_ID", csvRecord.get("ACK_ID"));
            recordMap.put("FORM_PLAN_YEAR_BEGIN_DATE", csvRecord.get("FORM_PLAN_YEAR_BEGIN_DATE"));
            recordMap.put("FORM_TAX_PRD", csvRecord.get("FORM_TAX_PRD"));
            recordMap.put("TYPE_PLAN_ENTITY_CD", csvRecord.get("TYPE_PLAN_ENTITY_CD"));
            recordMap.put("TYPE_DFE_PLAN_ENTITY_CD", csvRecord.get("TYPE_DFE_PLAN_ENTITY_CD"));
            recordMap.put("INITIAL_FILING_IND", csvRecord.get("INITIAL_FILING_IND"));
            recordMap.put("AMENDED_IND", csvRecord.get("AMENDED_IND"));
            recordMap.put("FINAL_FILING_IND", csvRecord.get("FINAL_FILING_IND"));
            recordMap.put("SHORT_PLAN_YR_IND", csvRecord.get("SHORT_PLAN_YR_IND"));
            recordMap.put("COLLECTIVE_BARGAIN_IND", csvRecord.get("COLLECTIVE_BARGAIN_IND"));
            recordMap.put("F5558_APPLICATION_FILED_IND", csvRecord.get("F5558_APPLICATION_FILED_IND"));
            recordMap.put("EXT_AUTOMATIC_IND", csvRecord.get("EXT_AUTOMATIC_IND"));
            recordMap.put("DFVC_PROGRAM_IND", csvRecord.get("DFVC_PROGRAM_IND"));
            recordMap.put("EXT_SPECIAL_IND", csvRecord.get("EXT_SPECIAL_IND"));
            recordMap.put("EXT_SPECIAL_TEXT", csvRecord.get("EXT_SPECIAL_TEXT"));
            recordMap.put("PLAN_NAME", csvRecord.get("PLAN_NAME"));
            recordMap.put("SPONS_DFE_PN", csvRecord.get("SPONS_DFE_PN"));
            recordMap.put("PLAN_EFF_DATE", csvRecord.get("PLAN_EFF_DATE"));
            recordMap.put("SPONSOR_DFE_NAME", csvRecord.get("SPONSOR_DFE_NAME"));
            recordMap.put("SPONS_DFE_DBA_NAME", csvRecord.get("SPONS_DFE_DBA_NAME"));
            recordMap.put("SPONS_DFE_CARE_OF_NAME", csvRecord.get("SPONS_DFE_CARE_OF_NAME"));
            recordMap.put("SPONS_DFE_MAIL_US_ADDRESS1", csvRecord.get("SPONS_DFE_MAIL_US_ADDRESS1"));
            recordMap.put("SPONS_DFE_MAIL_US_ADDRESS2", csvRecord.get("SPONS_DFE_MAIL_US_ADDRESS2"));
            recordMap.put("SPONS_DFE_MAIL_US_CITY", csvRecord.get("SPONS_DFE_MAIL_US_CITY"));
            recordMap.put("SPONS_DFE_MAIL_US_STATE", csvRecord.get("SPONS_DFE_MAIL_US_STATE"));
            recordMap.put("SPONS_DFE_MAIL_US_ZIP", csvRecord.get("SPONS_DFE_MAIL_US_ZIP"));
            recordMap.put("SPONS_DFE_MAIL_FOREIGN_ADDR1", csvRecord.get("SPONS_DFE_MAIL_FOREIGN_ADDR1"));
            recordMap.put("SPONS_DFE_MAIL_FOREIGN_ADDR2", csvRecord.get("SPONS_DFE_MAIL_FOREIGN_ADDR2"));
            recordMap.put("SPONS_DFE_MAIL_FOREIGN_CITY", csvRecord.get("SPONS_DFE_MAIL_FOREIGN_CITY"));
            recordMap.put("SPONS_DFE_MAIL_FORGN_PROV_ST", csvRecord.get("SPONS_DFE_MAIL_FORGN_PROV_ST"));
            recordMap.put("SPONS_DFE_MAIL_FOREIGN_CNTRY", csvRecord.get("SPONS_DFE_MAIL_FOREIGN_CNTRY"));
            recordMap.put("SPONS_DFE_MAIL_FORGN_POSTAL_CD", csvRecord.get("SPONS_DFE_MAIL_FORGN_POSTAL_CD"));
            recordMap.put("SPONS_DFE_LOC_US_ADDRESS1", csvRecord.get("SPONS_DFE_LOC_US_ADDRESS1"));
            recordMap.put("SPONS_DFE_LOC_US_ADDRESS2", csvRecord.get("SPONS_DFE_LOC_US_ADDRESS2"));
            recordMap.put("SPONS_DFE_LOC_US_CITY", csvRecord.get("SPONS_DFE_LOC_US_CITY"));
            recordMap.put("SPONS_DFE_LOC_US_STATE", csvRecord.get("SPONS_DFE_LOC_US_STATE"));
            recordMap.put("SPONS_DFE_LOC_US_ZIP", csvRecord.get("SPONS_DFE_LOC_US_ZIP"));
            recordMap.put("SPONS_DFE_LOC_FOREIGN_ADDRESS1", csvRecord.get("SPONS_DFE_LOC_FOREIGN_ADDRESS1"));
            recordMap.put("SPONS_DFE_LOC_FOREIGN_ADDRESS2", csvRecord.get("SPONS_DFE_LOC_FOREIGN_ADDRESS2"));
            recordMap.put("SPONS_DFE_LOC_FOREIGN_CITY", csvRecord.get("SPONS_DFE_LOC_FOREIGN_CITY"));
            recordMap.put("SPONS_DFE_LOC_FORGN_PROV_ST", csvRecord.get("SPONS_DFE_LOC_FORGN_PROV_ST"));
            recordMap.put("SPONS_DFE_LOC_FOREIGN_CNTRY", csvRecord.get("SPONS_DFE_LOC_FOREIGN_CNTRY"));
            recordMap.put("SPONS_DFE_LOC_FORGN_POSTAL_CD", csvRecord.get("SPONS_DFE_LOC_FORGN_POSTAL_CD"));
            recordMap.put("SPONS_DFE_EIN", csvRecord.get("SPONS_DFE_EIN"));
            recordMap.put("SPONS_DFE_PHONE_NUM", csvRecord.get("SPONS_DFE_PHONE_NUM"));
            recordMap.put("BUSINESS_CODE", csvRecord.get("BUSINESS_CODE"));
            recordMap.put("ADMIN_NAME", csvRecord.get("ADMIN_NAME"));
            recordMap.put("ADMIN_CARE_OF_NAME", csvRecord.get("ADMIN_CARE_OF_NAME"));
            recordMap.put("ADMIN_US_ADDRESS1", csvRecord.get("ADMIN_US_ADDRESS1"));
            recordMap.put("ADMIN_US_ADDRESS2", csvRecord.get("ADMIN_US_ADDRESS2"));
            recordMap.put("ADMIN_US_CITY", csvRecord.get("ADMIN_US_CITY"));
            recordMap.put("ADMIN_US_STATE", csvRecord.get("ADMIN_US_STATE"));
            recordMap.put("ADMIN_US_ZIP", csvRecord.get("ADMIN_US_ZIP"));
            recordMap.put("ADMIN_FOREIGN_ADDRESS1", csvRecord.get("ADMIN_FOREIGN_ADDRESS1"));
            recordMap.put("ADMIN_FOREIGN_ADDRESS2", csvRecord.get("ADMIN_FOREIGN_ADDRESS2"));
            recordMap.put("ADMIN_FOREIGN_CITY", csvRecord.get("ADMIN_FOREIGN_CITY"));
            recordMap.put("ADMIN_FOREIGN_PROV_STATE", csvRecord.get("ADMIN_FOREIGN_PROV_STATE"));
            recordMap.put("ADMIN_FOREIGN_CNTRY", csvRecord.get("ADMIN_FOREIGN_CNTRY"));
            recordMap.put("ADMIN_FOREIGN_POSTAL_CD", csvRecord.get("ADMIN_FOREIGN_POSTAL_CD"));
            recordMap.put("ADMIN_EIN", csvRecord.get("ADMIN_EIN"));
            recordMap.put("ADMIN_PHONE_NUM", csvRecord.get("ADMIN_PHONE_NUM"));
            recordMap.put("LAST_RPT_SPONS_NAME", csvRecord.get("LAST_RPT_SPONS_NAME"));
            recordMap.put("LAST_RPT_SPONS_EIN", csvRecord.get("LAST_RPT_SPONS_EIN"));
            recordMap.put("LAST_RPT_PLAN_NUM", csvRecord.get("LAST_RPT_PLAN_NUM"));
            recordMap.put("ADMIN_SIGNED_DATE", csvRecord.get("ADMIN_SIGNED_DATE"));
            recordMap.put("ADMIN_SIGNED_NAME", csvRecord.get("ADMIN_SIGNED_NAME"));
            recordMap.put("SPONS_SIGNED_DATE", csvRecord.get("SPONS_SIGNED_DATE"));
            recordMap.put("SPONS_SIGNED_NAME", csvRecord.get("SPONS_SIGNED_NAME"));
            recordMap.put("DFE_SIGNED_DATE", csvRecord.get("DFE_SIGNED_DATE"));
            recordMap.put("DFE_SIGNED_NAME", csvRecord.get("DFE_SIGNED_NAME"));
            recordMap.put("TOT_PARTCP_BOY_CNT", csvRecord.get("TOT_PARTCP_BOY_CNT"));
            recordMap.put("TOT_ACTIVE_PARTCP_CNT", csvRecord.get("TOT_ACTIVE_PARTCP_CNT"));
            recordMap.put("RTD_SEP_PARTCP_RCVG_CNT", csvRecord.get("RTD_SEP_PARTCP_RCVG_CNT"));
            recordMap.put("RTD_SEP_PARTCP_FUT_CNT", csvRecord.get("RTD_SEP_PARTCP_FUT_CNT"));
            recordMap.put("SUBTL_ACT_RTD_SEP_CNT", csvRecord.get("SUBTL_ACT_RTD_SEP_CNT"));
            recordMap.put("BENEF_RCVG_BNFT_CNT", csvRecord.get("BENEF_RCVG_BNFT_CNT"));
            recordMap.put("TOT_ACT_RTD_SEP_BENEF_CNT", csvRecord.get("TOT_ACT_RTD_SEP_BENEF_CNT"));
            recordMap.put("PARTCP_ACCOUNT_BAL_CNT", csvRecord.get("PARTCP_ACCOUNT_BAL_CNT"));
            recordMap.put("SEP_PARTCP_PARTL_VSTD_CNT", csvRecord.get("SEP_PARTCP_PARTL_VSTD_CNT"));
            recordMap.put("CONTRIB_EMPLRS_CNT", csvRecord.get("CONTRIB_EMPLRS_CNT"));
            recordMap.put("TYPE_PENSION_BNFT_CODE", csvRecord.get("TYPE_PENSION_BNFT_CODE"));
            recordMap.put("TYPE_WELFARE_BNFT_CODE", csvRecord.get("TYPE_WELFARE_BNFT_CODE"));
            recordMap.put("FUNDING_INSURANCE_IND", csvRecord.get("FUNDING_INSURANCE_IND"));
            recordMap.put("FUNDING_SEC412_IND", csvRecord.get("FUNDING_SEC412_IND"));
            recordMap.put("FUNDING_TRUST_IND", csvRecord.get("FUNDING_TRUST_IND"));
            recordMap.put("FUNDING_GEN_ASSET_IND", csvRecord.get("FUNDING_GEN_ASSET_IND"));
            recordMap.put("BENEFIT_INSURANCE_IND", csvRecord.get("BENEFIT_INSURANCE_IND"));
            recordMap.put("BENEFIT_SEC412_IND", csvRecord.get("BENEFIT_SEC412_IND"));
            recordMap.put("BENEFIT_TRUST_IND", csvRecord.get("BENEFIT_TRUST_IND"));
            recordMap.put("BENEFIT_GEN_ASSET_IND", csvRecord.get("BENEFIT_GEN_ASSET_IND"));
            recordMap.put("SCH_R_ATTACHED_IND", csvRecord.get("SCH_R_ATTACHED_IND"));
            recordMap.put("SCH_MB_ATTACHED_IND", csvRecord.get("SCH_MB_ATTACHED_IND"));
            recordMap.put("SCH_SB_ATTACHED_IND", csvRecord.get("SCH_SB_ATTACHED_IND"));
            recordMap.put("SCH_H_ATTACHED_IND", csvRecord.get("SCH_H_ATTACHED_IND"));
            recordMap.put("SCH_I_ATTACHED_IND", csvRecord.get("SCH_I_ATTACHED_IND"));
            recordMap.put("SCH_A_ATTACHED_IND", csvRecord.get("SCH_A_ATTACHED_IND"));
            recordMap.put("NUM_SCH_A_ATTACHED_CNT", csvRecord.get("NUM_SCH_A_ATTACHED_CNT"));
            recordMap.put("SCH_C_ATTACHED_IND", csvRecord.get("SCH_C_ATTACHED_IND"));
            recordMap.put("SCH_D_ATTACHED_IND", csvRecord.get("SCH_D_ATTACHED_IND"));
            recordMap.put("SCH_G_ATTACHED_IND", csvRecord.get("SCH_G_ATTACHED_IND"));
            recordMap.put("FILING_STATUS", csvRecord.get("FILING_STATUS"));
            recordMap.put("DATE_RECEIVED", csvRecord.get("DATE_RECEIVED"));
            recordMap.put("VALID_ADMIN_SIGNATURE", csvRecord.get("VALID_ADMIN_SIGNATURE"));
            recordMap.put("VALID_DFE_SIGNATURE", csvRecord.get("VALID_DFE_SIGNATURE"));
            recordMap.put("VALID_SPONSOR_SIGNATURE", csvRecord.get("VALID_SPONSOR_SIGNATURE"));
            recordMap.put("ADMIN_PHONE_NUM_FOREIGN", csvRecord.get("ADMIN_PHONE_NUM_FOREIGN"));
            recordMap.put("SPONS_DFE_PHONE_NUM_FOREIGN", csvRecord.get("SPONS_DFE_PHONE_NUM_FOREIGN"));
            recordMap.put("ADMIN_NAME_SAME_SPON_IND", csvRecord.get("ADMIN_NAME_SAME_SPON_IND"));
            recordMap.put("ADMIN_ADDRESS_SAME_SPON_IND", csvRecord.get("ADMIN_ADDRESS_SAME_SPON_IND"));
            recordMap.put("PREPARER_NAME", csvRecord.get("PREPARER_NAME"));
            recordMap.put("PREPARER_FIRM_NAME", csvRecord.get("PREPARER_FIRM_NAME"));
            recordMap.put("PREPARER_US_ADDRESS1", csvRecord.get("PREPARER_US_ADDRESS1"));
            recordMap.put("PREPARER_US_ADDRESS2", csvRecord.get("PREPARER_US_ADDRESS2"));
            recordMap.put("PREPARER_US_CITY", csvRecord.get("PREPARER_US_CITY"));
            recordMap.put("PREPARER_US_STATE", csvRecord.get("PREPARER_US_STATE"));
            recordMap.put("PREPARER_US_ZIP", csvRecord.get("PREPARER_US_ZIP"));
            recordMap.put("PREPARER_FOREIGN_ADDRESS1", csvRecord.get("PREPARER_FOREIGN_ADDRESS1"));
            recordMap.put("PREPARER_FOREIGN_ADDRESS2", csvRecord.get("PREPARER_FOREIGN_ADDRESS2"));
            recordMap.put("PREPARER_FOREIGN_CITY", csvRecord.get("PREPARER_FOREIGN_CITY"));
            recordMap.put("PREPARER_FOREIGN_PROV_STATE", csvRecord.get("PREPARER_FOREIGN_PROV_STATE"));
            recordMap.put("PREPARER_FOREIGN_CNTRY", csvRecord.get("PREPARER_FOREIGN_CNTRY"));
            recordMap.put("PREPARER_FOREIGN_POSTAL_CD", csvRecord.get("PREPARER_FOREIGN_POSTAL_CD"));
            recordMap.put("PREPARER_PHONE_NUM", csvRecord.get("PREPARER_PHONE_NUM"));
            recordMap.put("PREPARER_PHONE_NUM_FOREIGN", csvRecord.get("PREPARER_PHONE_NUM_FOREIGN"));
            recordMap.put("TOT_ACT_PARTCP_BOY_CNT", csvRecord.get("TOT_ACT_PARTCP_BOY_CNT"));
            recordMap.put("SUBJ_M1_FILING_REQ_IND", csvRecord.get("SUBJ_M1_FILING_REQ_IND"));
            recordMap.put("COMPLIANCE_M1_FILING_REQ_IND", csvRecord.get("COMPLIANCE_M1_FILING_REQ_IND"));
            recordMap.put("M1_RECEIPT_CONFIRMATION_CODE", csvRecord.get("M1_RECEIPT_CONFIRMATION_CODE"));
            recordMap.put("ADMIN_MANUAL_SIGNED_DATE", csvRecord.get("ADMIN_MANUAL_SIGNED_DATE"));
            recordMap.put("ADMIN_MANUAL_SIGNED_NAME", csvRecord.get("ADMIN_MANUAL_SIGNED_NAME"));
            recordMap.put("LAST_RPT_PLAN_NAME", csvRecord.get("LAST_RPT_PLAN_NAME"));
            recordMap.put("SPONS_MANUAL_SIGNED_DATE", csvRecord.get("SPONS_MANUAL_SIGNED_DATE"));
            recordMap.put("SPONS_MANUAL_SIGNED_NAME", csvRecord.get("SPONS_MANUAL_SIGNED_NAME"));
            recordMap.put("DFE_MANUAL_SIGNED_DATE", csvRecord.get("DFE_MANUAL_SIGNED_DATE"));
            recordMap.put("DFE_MANUAL_SIGNED_NAME", csvRecord.get("DFE_MANUAL_SIGNED_NAME"));
            recordList.add(recordMap);
        }
        System.out.println(recordList.size());
        elasticSearchFilingManager.processCsv(recordList);
//        System.out.println(FilingUtils.toJSON(recordList));
    }
}
