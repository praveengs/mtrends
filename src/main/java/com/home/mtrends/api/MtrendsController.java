package com.home.mtrends.api;

import com.home.mtrends.model.PlayCount;
import com.home.mtrends.service.TrendsService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/mtrends")
public class MtrendsController {

    private final TrendsService trendsService;

    public MtrendsController(TrendsService trendsService) {
        this.trendsService = trendsService;
    }

    @GetMapping("/")
    public List<PlayCount> GetTrends() {
        return trendsService.getTrends();
    }
}
