# WAR Lab People Analytics Dashboard

## Overview

A production-quality, interactive single-page HTML application for People Analytics. The dashboard provides comprehensive HR metrics with professional dark-themed visualizations using Apache ECharts.

## Files

- **index.html** (53 KB) - Main dashboard application
- **error.html** (7.3 KB) - 404 error page
- **README.md** - This file

## Features

### Architecture
- **Tech Stack**: Pure HTML/CSS/JavaScript with Apache ECharts 5 from CDN
- **Self-Contained**: All CSS and JavaScript inline, no external dependencies except ECharts
- **Responsive**: Adapts to desktop, tablet, and mobile viewports
- **Dark Theme**: Professional color scheme optimized for readability

### Dashboard Tabs

#### 1. Overview (KPI Summary)
Five key performance indicator cards displaying:
- Active Headcount
- Total Movements (Hires + Terminations + Transfers)
- Average Base Pay
- Active Companies
- Active Departments

Each card includes trend indicators and formatted values.

#### 2. Headcount Analysis
Four complementary visualizations:
- **Stacked Bar Chart**: Headcount distribution by Company
- **Donut Chart**: Worker Type distribution (Full-Time, Part-Time, Contract)
- **Horizontal Bar Chart**: Headcount by Department (top 20, sortable)
- **Line Chart**: 12-month headcount trend

#### 3. Movements & Attrition
- **KPI Cards**: Turnover Rate, Regrettable Terminations, Net Change
- **Waterfall Chart**: Hires vs Terminations (shows net hiring flow)
- **Horizontal Bar**: Movement types breakdown
- **Stacked Area**: Monthly trend of hire/termination/transfer activity

#### 4. Compensation Analysis
- **KPI Cards**: Avg Compa-Ratio, Median Base Pay, Pay Range Width
- **Horizontal Bar**: Average Base Pay by Job Family
- **Box Plot**: Compensation distribution by Grade (min/Q1/median/Q3/max)

#### 5. Organizational Health
- **Horizontal Bar**: Top 15 largest departments by headcount
- **Donut Chart**: Span of Control distribution
- **Horizontal Bar**: Location distribution

### Data Loading

The dashboard intelligently handles data:

**Production Mode** (with Lambda):
- Fetches pre-aggregated JSON from `/data/` endpoints:
  - `/data/kpi_summary.json`
  - `/data/headcount.json`
  - `/data/movements.json`
  - `/data/compensation.json`
  - `/data/org_health.json`

**Demo Mode** (fallback):
- Automatically generates realistic demo data if fetch fails
- Includes 5 companies, 20 departments, 12 locations
- ~2,500 headcount with 12 months of trends
- Compensation ranges: $40K - $250K
- Status indicator shows when demo data is active

### User Experience Features

- **Auto-Refresh**: Polls for updated data every 5 minutes
- **Live Indicator**: Shows "Live" status with pulsing dot
- **Current Date/Time**: Updates in real-time
- **Export Charts**: Download any chart as PNG (ECharts built-in)
- **Tab Navigation**: Sticky tabs with smooth transitions
- **Responsive Grid**: Auto-adapts to screen size
- **Hover Effects**: Cards lift and highlight on hover
- **Loading States**: Skeleton animations during data load

### Styling Details

**Color Palette**:
- Primary Background: `#1a1a2e` (dark navy)
- Secondary: `#16213e` (slightly lighter navy)
- Accent: `#0f3460` (deep blue)
- Highlight: `#e94560` (vibrant pink/red)
- Success: `#00d9ff` (cyan)
- Warning: `#ffa502` (orange)

**Design Elements**:
- Subtle shadows and depth
- Rounded corners (12px on cards, 8px on buttons)
- Smooth CSS transitions (0.3s ease)
- Professional sans-serif font stack
- Dark mode optimized for reduced eye strain
- Gradient text on headings
- Grid-based responsive layout

## Configuration

### ECharts Settings

All charts configured for dark theme with:
- Custom color schemes matching dashboard palette
- Dark backgrounds and subtle gridlines
- Hover animations and tooltips
- Responsive sizing
- PNG export capability

### Data Schema

Expected JSON structure from Lambda:

```json
{
  "kpiSummary": {
    "activeHeadcount": 2450,
    "totalMovements": 187,
    "avgBasePay": 125000,
    "avgBasePayPrev": 122000,
    "activeCompanies": 5,
    "activeDepartments": 20,
    "turnoverRate": 0.089,
    "regrettableTerms": 12,
    "netChange": 45,
    "avgCompaRatio": 0.95,
    "medianPay": 115000,
    "payRangeWidth": 1.45
  },
  "headcount": {
    "headcountByCompany": [...],
    "headcountByDept": [...],
    "workerTypeData": {...},
    "headcountTrend": [...]
  }
}
```

## Deployment

### S3 + CloudFront Setup (Current Deployment)

**Infrastructure IDs:**
- S3 Bucket: `warlab-hr-dashboard`
- CloudFront Distribution: `E3RGFB9ROIS4KH`
- CloudFront Domain: `d142tokwl5q6ig.cloudfront.net`
- Lambda Extractor: `warlab-dashboard-extractor`
- Redshift Cluster: `warlab-hr-datamart` (database: `dev`, schema: `l3_workday`)

**Deploy dashboard HTML:**
```bash
aws s3 cp index.html s3://warlab-hr-dashboard/index.html --content-type "text/html"
aws s3 cp error.html s3://warlab-hr-dashboard/error.html --content-type "text/html"
aws cloudfront create-invalidation --distribution-id E3RGFB9ROIS4KH --paths "/*"
```

**Deploy Lambda function:**
```bash
# Package from artifacts/lambda/dashboard_extractor/
cd artifacts/lambda/dashboard_extractor
pip install redshift_connector -t .
zip -r lambda-package.zip .
aws s3 cp lambda-package.zip s3://warlab-hr-datamart-dev/lambda/lambda-package.zip
aws lambda update-function-code --function-name warlab-dashboard-extractor \
    --s3-bucket warlab-hr-datamart-dev --s3-key lambda/lambda-package.zip
```

**Run data extractions (all 5):**
```bash
for ext in kpi_summary headcount movements compensation org_health; do
    printf '{"extraction":"%s"}' "$ext" > /tmp/payload.json
    aws lambda invoke --function-name warlab-dashboard-extractor \
        --payload fileb:///tmp/payload.json \
        --cli-binary-format raw-in-base64-out /tmp/response.json
done
```

**Data endpoints** (served from S3 via CloudFront):
```
/data/kpi_summary.json
/data/headcount.json
/data/movements.json
/data/compensation.json
/data/org_health.json
```

### Data Flow

```
Redshift (l3_workday schema)
  └─→ Lambda (warlab-dashboard-extractor)
       └─→ S3 (warlab-hr-dashboard/data/*.json)
            └─→ CloudFront (d142tokwl5q6ig.cloudfront.net)
                 └─→ Dashboard (index.html fetches /data/*.json)
```

The Lambda outputs snake_case JSON. The dashboard's `transformLambdaData()` function maps these to camelCase for internal use.

**Key Lambda query notes (Issues 14-15):**
- Headcount by_company and by_location use **natural key JOINs** (company_id, location_id), not surrogate keys
- Headcount by_department and org_health departments query **dim_worker_job_d directly**, grouping by `supervisory_organization` (CC format)
- This avoids NULL surrogate key issues and DPT-vs-CC ID format mismatches

### Environment Requirements

- CORS enabled on Lambda/S3 for cross-origin data fetch
- TLS/HTTPS enforced
- Proper MIME types: `text/html`, `application/json`

## Performance

- **Page Load**: <2 seconds (with cached assets)
- **Chart Render**: <500ms per visualization
- **Data Refresh**: 5-minute interval (configurable)
- **Memory**: ~15-20 MB with all charts rendered
- **Browser Support**: All modern browsers (Chrome, Firefox, Safari, Edge)

## Customization

### Modifying Colors
Edit CSS variables in `<style>`:
```css
:root {
    --color-primary: #1a1a2e;
    --color-highlight: #e94560;
    ...
}
```

### Changing Refresh Interval
```javascript
CONFIG.refreshInterval = 300000; // milliseconds
```

### Adding New Tabs
1. Add HTML tab content
2. Add navigation button
3. Create render function
4. Register in `renderAllTabs()`

### Demo Data Parameters
Edit `generateDemoData()` function to adjust:
- Number of companies/departments
- Headcount range
- Pay ranges
- Trend data

## Browser Compatibility

- Chrome 90+
- Firefox 88+
- Safari 14+
- Edge 90+
- Mobile browsers (iOS Safari, Chrome Mobile)

## Security

- No external data transmission beyond configured endpoints
- No local storage of sensitive data
- CSP-compatible inline scripts
- XSS protection via ECharts sanitization
- HTTPS-only in production

## Error Handling

- Graceful fallback to demo data
- Console error logging
- User-friendly error messages
- HTTP status page (error.html)

## Future Enhancements

- Real-time WebSocket updates
- Custom date range filters
- Drill-down capabilities
- User preferences (theme, layout)
- Data export (CSV, Excel)
- Custom dashboard builder
- Mobile app integration
- Multi-language support

## Support

For issues or questions:
1. Check browser console for errors
2. Verify Lambda data endpoints are accessible
3. Confirm CORS configuration
4. Review data schema matches expected format

---

**Version**: 1.1.0
**Last Updated**: February 2026
**Maintained By**: WAR Lab Analytics Team
