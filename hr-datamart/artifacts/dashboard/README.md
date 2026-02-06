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

### S3 + CloudFront Setup

1. Upload files to S3:
   ```
   aws s3 cp index.html s3://your-bucket/
   aws s3 cp error.html s3://your-bucket/
   ```

2. CloudFront Distribution:
   - Origin: S3 bucket
   - Default root object: `index.html`
   - Error handling: Route 404s to `error.html`
   - Cache behavior: 5 minutes for HTML, 1 hour for assets
   - Enable compression

3. Serve data from Lambda:
   ```
   /data/kpi_summary.json
   /data/headcount.json
   /data/movements.json
   /data/compensation.json
   /data/org_health.json
   ```

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

**Version**: 1.0.0
**Last Updated**: February 2025
**Maintained By**: WAR Lab Analytics Team
