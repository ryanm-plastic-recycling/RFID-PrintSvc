# Direct-ZPL Template Source of Truth

Production direct-ZPL templates live in `ZPL_TEMPLATE_SOURCE_DIR`, which defaults to `C:\RFID\zpl`. Template Lab reads approved `.template.zpl` files from that same directory, but rendered proof downloads must not be copied over production templates because they contain literal sample values.

To create a station-specific dynamic template from an existing BarTender `.btw` file:

1. Open the station `.btw` in BarTender and print/export a ZPL/PRN proof to a scratch location, not `C:\RFID\zpl`.
2. Convert the proof into a `.template.zpl` file named for the station, such as `RFID-RAW-P4.template.zpl` or `RFID-FG-P4.template.zpl`.
3. Replace every sample value with the PrintSvc tokens used by the existing templates, including `{{lotNumber}}`, `{{boxNumber}}`, `{{rfid}}`, `{{rfidHex}}`, `{{pounds}}`, `{{po}}`, `{{productCode}}`, `{{productDescription}}`, and the fitted field tokens such as `{{colorText}}`, `{{materialTypeText}}`, and `{{productDescriptionText}}`.
4. Keep RFID commands dynamic. The EPC write should use `{{rfidHex}}`; do not paste rendered sample RFID hex.
5. Keep the QR payload dynamic. RFID production templates should encode `{{lotNumber}}` in the QR command.
6. If the BarTender proof contains a low-resolution logo bitmap, replace it through Template Lab's controlled logo workflow. Upload a higher-quality PNG, convert it to `^GFA`, and store the asset under `C:\RFID\zpl\assets`.
7. Save the dynamic file under `C:\RFID\zpl` or deploy it with `Deploy-ZPL-Templates.bat`.
8. Run `GET /api/print/zpl-template-validation` and fix missing, tokenless, or wrong-station mappings before enabling a station/family scope.

Template Lab profile saves only update `template-lab-profiles.json`. To change production, use the explicit **Promote Dynamic Template to Production** action, which backs up the current dynamic template and writes token-preserving source changes.

Calibration notes:

- Use **Print calibration grid** before moving a P5/P6/P7/P8 template heavily. The grid prints the outer border, center lines, rulers, and corner labels to reveal media clipping or printer home/shift behavior.
- Whole-label offsets are printer dots. Prefer `globalOffsetX/globalOffsetY` first; use `^LH`, `^LS`, or `^LT` only as secondary printer-specific adjustments.
- Keep `Scale border thickness with label scale` off unless the stroke itself needs to grow. With the toggle off, `^GB` positions and dimensions scale while border thickness stays fixed.
