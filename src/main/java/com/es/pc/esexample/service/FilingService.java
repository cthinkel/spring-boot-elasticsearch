package com.es.pc.esexample.service;

import com.es.pc.esexample.manager.ElasticSearchFilingManager;

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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
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
        BufferedReader br;
        List<String> result = new ArrayList<>();
        try {

            String line;
            InputStream is = file.getInputStream();
            br = new BufferedReader(new InputStreamReader(is));
            while ((line = br.readLine()) != null) {
                result.add(line);
            }
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
        System.out.println(result.size());
        elasticSearchFilingManager.processCsv(result);
    }
}
