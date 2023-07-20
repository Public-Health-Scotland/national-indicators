select s.link_no, s.cis_marker, s.admission_date,
  case when s.discharge_date is null then d.date_of_death else s.discharge_date
  end discharge_date,
d.date_of_death,
  case when s.location in ('A240V','F821V','G105V','G518V','G203V','G315V','G424V','G541V','G557V','H239V','L112V','L213V','L215V','L330V','L365V','N465R','N498V','S312R','S327V','T315S','T337V','Y121V') then 1 else 0
  end ch_flag
from analysis.smr04_pi s, analysis.gro_deaths_c d
where s.link_no = d.link_no
  and s.management_of_patient in ('1', '3', '5', '7', 'A')
  and (s.discharge_date between '{extract_start_smr}' and '{extract_end}' or discharge_date is null)
  and ((substr(d.underlying_cause_of_death, 1, 3) not in ('V01','V02','V03','V04','V05','V06','V07','V08','V09','V10','V11','V12','V13','V14','V15','V16','V17','V18','V19','V20','V21','V22','V23','V24','V25','V26','V27','V28','V29','V30','V31','V32','V33','V34','V35','V36','V37','V38','V39','V40','V41','V42','V43','V44','V45','V46','V47','V48','V49','V50','V51','V52','V53','V54','V55','V56','V57','V58','V59','V60','V61','V62','V63','V64','V65','V66','V67','V68','V69','V70','V71','V72','V73','V74','V75','V76','V77','V78','V79','V80','V81','V82','V83','V84','V85','V86','V87','V88','V89','V90','V91','V92','V93','V94','V95','V96','V97','V98','V99','W20','W21','W22','W23','W24','W25','W26','W27','W28','W29','W30','W31','W32','W33','W34','W35','W36','W37','W38','W39','W40','W41','W42','W43','W44','W45','W46','W47','W48','W49','W50','W51','W52','W53','W54','W55','W56','W57','W58','W59','W60','W61','W62','W63','W64','W65','W66','W67','W68','W69','W70','W71','W72','W73','W74','W75','W76','W77','W78','W79','W80','W81','W82','W83','W84','W85','W86','W87','W88','W89','W90','W91','W92','W93','W94','W95','W96','W97','W98','W99','X00','X01','X02','X03','X04','X05','X06','X07','X08','X09','X10','X11','X12','X13','X14','X15','X16','X17','X18','X19','X20','X21','X22','X23','X24','X25','X26','X27','X28','X29','X30','X31','X32','X33','X34','X35','X36','X37','X38','X39','X40','X41','X42','X43','X44','X45','X46','X47','X48','X49','X50','X51','X52','X53','X54','X55','X56','X57','X58','X59','X60','X61','X62','X63','X64','X65','X66','X67','X68','X69','X70','X71','X72','X73','X74','X75','X76','X77','X78','X79','X80','X81','X82','X83','X84','X85','X86','X87','X88','X89','X90','X91','X92','X93','X94','X95','X96','X97','X98','X99','Y00','Y01','Y02','Y03','Y04','Y05','Y06','Y07','Y08','Y09','Y10','Y11','Y12','Y13','Y14','Y15','Y16','Y17','Y18','Y19','Y20','Y21','Y22','Y23','Y24','Y25','Y26','Y27','Y28','Y29','Y30','Y31','Y32','Y33','Y34','Y35','Y36','Y37','Y38','Y39','Y40','Y41','Y42','Y43','Y44','Y45','Y46','Y47','Y48','Y49','Y50','Y51','Y52','Y53','Y54','Y55','Y56','Y57','Y58','Y59','Y60','Y61','Y62','Y63','Y64','Y65','Y66','Y67','Y68','Y69','Y70','Y71','Y72','Y73','Y74','Y75','Y76','Y77','Y78','Y79','Y80','Y81','Y82','Y83','Y84')
  and (substr(d.cause_of_death_code_0, 1, 3) is null
    or substr(d.cause_of_death_code_0, 1, 3) not in ('V01','V02','V03','V04','V05','V06','V07','V08','V09','V10','V11','V12','V13','V14','V15','V16','V17','V18','V19','V20','V21','V22','V23','V24','V25','V26','V27','V28','V29','V30','V31','V32','V33','V34','V35','V36','V37','V38','V39','V40','V41','V42','V43','V44','V45','V46','V47','V48','V49','V50','V51','V52','V53','V54','V55','V56','V57','V58','V59','V60','V61','V62','V63','V64','V65','V66','V67','V68','V69','V70','V71','V72','V73','V74','V75','V76','V77','V78','V79','V80','V81','V82','V83','V84','V85','V86','V87','V88','V89','V90','V91','V92','V93','V94','V95','V96','V97','V98','V99','W20','W21','W22','W23','W24','W25','W26','W27','W28','W29','W30','W31','W32','W33','W34','W35','W36','W37','W38','W39','W40','W41','W42','W43','W44','W45','W46','W47','W48','W49','W50','W51','W52','W53','W54','W55','W56','W57','W58','W59','W60','W61','W62','W63','W64','W65','W66','W67','W68','W69','W70','W71','W72','W73','W74','W75','W76','W77','W78','W79','W80','W81','W82','W83','W84','W85','W86','W87','W88','W89','W90','W91','W92','W93','W94','W95','W96','W97','W98','W99','X00','X01','X02','X03','X04','X05','X06','X07','X08','X09','X10','X11','X12','X13','X14','X15','X16','X17','X18','X19','X20','X21','X22','X23','X24','X25','X26','X27','X28','X29','X30','X31','X32','X33','X34','X35','X36','X37','X38','X39','X40','X41','X42','X43','X44','X45','X46','X47','X48','X49','X50','X51','X52','X53','X54','X55','X56','X57','X58','X59','X60','X61','X62','X63','X64','X65','X66','X67','X68','X69','X70','X71','X72','X73','X74','X75','X76','X77','X78','X79','X80','X81','X82','X83','X84','X85','X86','X87','X88','X89','X90','X91','X92','X93','X94','X95','X96','X97','X98','X99','Y00','Y01','Y02','Y03','Y04','Y05','Y06','Y07','Y08','Y09','Y10','Y11','Y12','Y13','Y14','Y15','Y16','Y17','Y18','Y19','Y20','Y21','Y22','Y23','Y24','Y25','Y26','Y27','Y28','Y29','Y30','Y31','Y32','Y33','Y34','Y35','Y36','Y37','Y38','Y39','Y40','Y41','Y42','Y43','Y44','Y45','Y46','Y47','Y48','Y49','Y50','Y51','Y52','Y53','Y54','Y55','Y56','Y57','Y58','Y59','Y60','Y61','Y62','Y63','Y64','Y65','Y66','Y67','Y68','Y69','Y70','Y71','Y72','Y73','Y74','Y75','Y76','Y77','Y78','Y79','Y80','Y81','Y82','Y83','Y84'))
  and (substr(d.cause_of_death_code_1, 1, 3) is null
    or substr(d.cause_of_death_code_1, 1, 3) not in ('V01','V02','V03','V04','V05','V06','V07','V08','V09','V10','V11','V12','V13','V14','V15','V16','V17','V18','V19','V20','V21','V22','V23','V24','V25','V26','V27','V28','V29','V30','V31','V32','V33','V34','V35','V36','V37','V38','V39','V40','V41','V42','V43','V44','V45','V46','V47','V48','V49','V50','V51','V52','V53','V54','V55','V56','V57','V58','V59','V60','V61','V62','V63','V64','V65','V66','V67','V68','V69','V70','V71','V72','V73','V74','V75','V76','V77','V78','V79','V80','V81','V82','V83','V84','V85','V86','V87','V88','V89','V90','V91','V92','V93','V94','V95','V96','V97','V98','V99','W20','W21','W22','W23','W24','W25','W26','W27','W28','W29','W30','W31','W32','W33','W34','W35','W36','W37','W38','W39','W40','W41','W42','W43','W44','W45','W46','W47','W48','W49','W50','W51','W52','W53','W54','W55','W56','W57','W58','W59','W60','W61','W62','W63','W64','W65','W66','W67','W68','W69','W70','W71','W72','W73','W74','W75','W76','W77','W78','W79','W80','W81','W82','W83','W84','W85','W86','W87','W88','W89','W90','W91','W92','W93','W94','W95','W96','W97','W98','W99','X00','X01','X02','X03','X04','X05','X06','X07','X08','X09','X10','X11','X12','X13','X14','X15','X16','X17','X18','X19','X20','X21','X22','X23','X24','X25','X26','X27','X28','X29','X30','X31','X32','X33','X34','X35','X36','X37','X38','X39','X40','X41','X42','X43','X44','X45','X46','X47','X48','X49','X50','X51','X52','X53','X54','X55','X56','X57','X58','X59','X60','X61','X62','X63','X64','X65','X66','X67','X68','X69','X70','X71','X72','X73','X74','X75','X76','X77','X78','X79','X80','X81','X82','X83','X84','X85','X86','X87','X88','X89','X90','X91','X92','X93','X94','X95','X96','X97','X98','X99','Y00','Y01','Y02','Y03','Y04','Y05','Y06','Y07','Y08','Y09','Y10','Y11','Y12','Y13','Y14','Y15','Y16','Y17','Y18','Y19','Y20','Y21','Y22','Y23','Y24','Y25','Y26','Y27','Y28','Y29','Y30','Y31','Y32','Y33','Y34','Y35','Y36','Y37','Y38','Y39','Y40','Y41','Y42','Y43','Y44','Y45','Y46','Y47','Y48','Y49','Y50','Y51','Y52','Y53','Y54','Y55','Y56','Y57','Y58','Y59','Y60','Y61','Y62','Y63','Y64','Y65','Y66','Y67','Y68','Y69','Y70','Y71','Y72','Y73','Y74','Y75','Y76','Y77','Y78','Y79','Y80','Y81','Y82','Y83','Y84'))
  and (substr(d.cause_of_death_code_2, 1, 3) is null
    or substr(d.cause_of_death_code_2, 1, 3) not in ('V01','V02','V03','V04','V05','V06','V07','V08','V09','V10','V11','V12','V13','V14','V15','V16','V17','V18','V19','V20','V21','V22','V23','V24','V25','V26','V27','V28','V29','V30','V31','V32','V33','V34','V35','V36','V37','V38','V39','V40','V41','V42','V43','V44','V45','V46','V47','V48','V49','V50','V51','V52','V53','V54','V55','V56','V57','V58','V59','V60','V61','V62','V63','V64','V65','V66','V67','V68','V69','V70','V71','V72','V73','V74','V75','V76','V77','V78','V79','V80','V81','V82','V83','V84','V85','V86','V87','V88','V89','V90','V91','V92','V93','V94','V95','V96','V97','V98','V99','W20','W21','W22','W23','W24','W25','W26','W27','W28','W29','W30','W31','W32','W33','W34','W35','W36','W37','W38','W39','W40','W41','W42','W43','W44','W45','W46','W47','W48','W49','W50','W51','W52','W53','W54','W55','W56','W57','W58','W59','W60','W61','W62','W63','W64','W65','W66','W67','W68','W69','W70','W71','W72','W73','W74','W75','W76','W77','W78','W79','W80','W81','W82','W83','W84','W85','W86','W87','W88','W89','W90','W91','W92','W93','W94','W95','W96','W97','W98','W99','X00','X01','X02','X03','X04','X05','X06','X07','X08','X09','X10','X11','X12','X13','X14','X15','X16','X17','X18','X19','X20','X21','X22','X23','X24','X25','X26','X27','X28','X29','X30','X31','X32','X33','X34','X35','X36','X37','X38','X39','X40','X41','X42','X43','X44','X45','X46','X47','X48','X49','X50','X51','X52','X53','X54','X55','X56','X57','X58','X59','X60','X61','X62','X63','X64','X65','X66','X67','X68','X69','X70','X71','X72','X73','X74','X75','X76','X77','X78','X79','X80','X81','X82','X83','X84','X85','X86','X87','X88','X89','X90','X91','X92','X93','X94','X95','X96','X97','X98','X99','Y00','Y01','Y02','Y03','Y04','Y05','Y06','Y07','Y08','Y09','Y10','Y11','Y12','Y13','Y14','Y15','Y16','Y17','Y18','Y19','Y20','Y21','Y22','Y23','Y24','Y25','Y26','Y27','Y28','Y29','Y30','Y31','Y32','Y33','Y34','Y35','Y36','Y37','Y38','Y39','Y40','Y41','Y42','Y43','Y44','Y45','Y46','Y47','Y48','Y49','Y50','Y51','Y52','Y53','Y54','Y55','Y56','Y57','Y58','Y59','Y60','Y61','Y62','Y63','Y64','Y65','Y66','Y67','Y68','Y69','Y70','Y71','Y72','Y73','Y74','Y75','Y76','Y77','Y78','Y79','Y80','Y81','Y82','Y83','Y84'))
  and (substr(d.cause_of_death_code_3, 1, 3) is null
    or substr(d.cause_of_death_code_3, 1, 3) not in ('V01','V02','V03','V04','V05','V06','V07','V08','V09','V10','V11','V12','V13','V14','V15','V16','V17','V18','V19','V20','V21','V22','V23','V24','V25','V26','V27','V28','V29','V30','V31','V32','V33','V34','V35','V36','V37','V38','V39','V40','V41','V42','V43','V44','V45','V46','V47','V48','V49','V50','V51','V52','V53','V54','V55','V56','V57','V58','V59','V60','V61','V62','V63','V64','V65','V66','V67','V68','V69','V70','V71','V72','V73','V74','V75','V76','V77','V78','V79','V80','V81','V82','V83','V84','V85','V86','V87','V88','V89','V90','V91','V92','V93','V94','V95','V96','V97','V98','V99','W20','W21','W22','W23','W24','W25','W26','W27','W28','W29','W30','W31','W32','W33','W34','W35','W36','W37','W38','W39','W40','W41','W42','W43','W44','W45','W46','W47','W48','W49','W50','W51','W52','W53','W54','W55','W56','W57','W58','W59','W60','W61','W62','W63','W64','W65','W66','W67','W68','W69','W70','W71','W72','W73','W74','W75','W76','W77','W78','W79','W80','W81','W82','W83','W84','W85','W86','W87','W88','W89','W90','W91','W92','W93','W94','W95','W96','W97','W98','W99','X00','X01','X02','X03','X04','X05','X06','X07','X08','X09','X10','X11','X12','X13','X14','X15','X16','X17','X18','X19','X20','X21','X22','X23','X24','X25','X26','X27','X28','X29','X30','X31','X32','X33','X34','X35','X36','X37','X38','X39','X40','X41','X42','X43','X44','X45','X46','X47','X48','X49','X50','X51','X52','X53','X54','X55','X56','X57','X58','X59','X60','X61','X62','X63','X64','X65','X66','X67','X68','X69','X70','X71','X72','X73','X74','X75','X76','X77','X78','X79','X80','X81','X82','X83','X84','X85','X86','X87','X88','X89','X90','X91','X92','X93','X94','X95','X96','X97','X98','X99','Y00','Y01','Y02','Y03','Y04','Y05','Y06','Y07','Y08','Y09','Y10','Y11','Y12','Y13','Y14','Y15','Y16','Y17','Y18','Y19','Y20','Y21','Y22','Y23','Y24','Y25','Y26','Y27','Y28','Y29','Y30','Y31','Y32','Y33','Y34','Y35','Y36','Y37','Y38','Y39','Y40','Y41','Y42','Y43','Y44','Y45','Y46','Y47','Y48','Y49','Y50','Y51','Y52','Y53','Y54','Y55','Y56','Y57','Y58','Y59','Y60','Y61','Y62','Y63','Y64','Y65','Y66','Y67','Y68','Y69','Y70','Y71','Y72','Y73','Y74','Y75','Y76','Y77','Y78','Y79','Y80','Y81','Y82','Y83','Y84'))
  and (substr(d.cause_of_death_code_4, 1, 3) is null
    or substr(d.cause_of_death_code_4, 1, 3) not in ('V01','V02','V03','V04','V05','V06','V07','V08','V09','V10','V11','V12','V13','V14','V15','V16','V17','V18','V19','V20','V21','V22','V23','V24','V25','V26','V27','V28','V29','V30','V31','V32','V33','V34','V35','V36','V37','V38','V39','V40','V41','V42','V43','V44','V45','V46','V47','V48','V49','V50','V51','V52','V53','V54','V55','V56','V57','V58','V59','V60','V61','V62','V63','V64','V65','V66','V67','V68','V69','V70','V71','V72','V73','V74','V75','V76','V77','V78','V79','V80','V81','V82','V83','V84','V85','V86','V87','V88','V89','V90','V91','V92','V93','V94','V95','V96','V97','V98','V99','W20','W21','W22','W23','W24','W25','W26','W27','W28','W29','W30','W31','W32','W33','W34','W35','W36','W37','W38','W39','W40','W41','W42','W43','W44','W45','W46','W47','W48','W49','W50','W51','W52','W53','W54','W55','W56','W57','W58','W59','W60','W61','W62','W63','W64','W65','W66','W67','W68','W69','W70','W71','W72','W73','W74','W75','W76','W77','W78','W79','W80','W81','W82','W83','W84','W85','W86','W87','W88','W89','W90','W91','W92','W93','W94','W95','W96','W97','W98','W99','X00','X01','X02','X03','X04','X05','X06','X07','X08','X09','X10','X11','X12','X13','X14','X15','X16','X17','X18','X19','X20','X21','X22','X23','X24','X25','X26','X27','X28','X29','X30','X31','X32','X33','X34','X35','X36','X37','X38','X39','X40','X41','X42','X43','X44','X45','X46','X47','X48','X49','X50','X51','X52','X53','X54','X55','X56','X57','X58','X59','X60','X61','X62','X63','X64','X65','X66','X67','X68','X69','X70','X71','X72','X73','X74','X75','X76','X77','X78','X79','X80','X81','X82','X83','X84','X85','X86','X87','X88','X89','X90','X91','X92','X93','X94','X95','X96','X97','X98','X99','Y00','Y01','Y02','Y03','Y04','Y05','Y06','Y07','Y08','Y09','Y10','Y11','Y12','Y13','Y14','Y15','Y16','Y17','Y18','Y19','Y20','Y21','Y22','Y23','Y24','Y25','Y26','Y27','Y28','Y29','Y30','Y31','Y32','Y33','Y34','Y35','Y36','Y37','Y38','Y39','Y40','Y41','Y42','Y43','Y44','Y45','Y46','Y47','Y48','Y49','Y50','Y51','Y52','Y53','Y54','Y55','Y56','Y57','Y58','Y59','Y60','Y61','Y62','Y63','Y64','Y65','Y66','Y67','Y68','Y69','Y70','Y71','Y72','Y73','Y74','Y75','Y76','Y77','Y78','Y79','Y80','Y81','Y82','Y83','Y84'))
  and (substr(d.cause_of_death_code_5, 1, 3) is null
    or substr(d.cause_of_death_code_5, 1, 3) not in ('V01','V02','V03','V04','V05','V06','V07','V08','V09','V10','V11','V12','V13','V14','V15','V16','V17','V18','V19','V20','V21','V22','V23','V24','V25','V26','V27','V28','V29','V30','V31','V32','V33','V34','V35','V36','V37','V38','V39','V40','V41','V42','V43','V44','V45','V46','V47','V48','V49','V50','V51','V52','V53','V54','V55','V56','V57','V58','V59','V60','V61','V62','V63','V64','V65','V66','V67','V68','V69','V70','V71','V72','V73','V74','V75','V76','V77','V78','V79','V80','V81','V82','V83','V84','V85','V86','V87','V88','V89','V90','V91','V92','V93','V94','V95','V96','V97','V98','V99','W20','W21','W22','W23','W24','W25','W26','W27','W28','W29','W30','W31','W32','W33','W34','W35','W36','W37','W38','W39','W40','W41','W42','W43','W44','W45','W46','W47','W48','W49','W50','W51','W52','W53','W54','W55','W56','W57','W58','W59','W60','W61','W62','W63','W64','W65','W66','W67','W68','W69','W70','W71','W72','W73','W74','W75','W76','W77','W78','W79','W80','W81','W82','W83','W84','W85','W86','W87','W88','W89','W90','W91','W92','W93','W94','W95','W96','W97','W98','W99','X00','X01','X02','X03','X04','X05','X06','X07','X08','X09','X10','X11','X12','X13','X14','X15','X16','X17','X18','X19','X20','X21','X22','X23','X24','X25','X26','X27','X28','X29','X30','X31','X32','X33','X34','X35','X36','X37','X38','X39','X40','X41','X42','X43','X44','X45','X46','X47','X48','X49','X50','X51','X52','X53','X54','X55','X56','X57','X58','X59','X60','X61','X62','X63','X64','X65','X66','X67','X68','X69','X70','X71','X72','X73','X74','X75','X76','X77','X78','X79','X80','X81','X82','X83','X84','X85','X86','X87','X88','X89','X90','X91','X92','X93','X94','X95','X96','X97','X98','X99','Y00','Y01','Y02','Y03','Y04','Y05','Y06','Y07','Y08','Y09','Y10','Y11','Y12','Y13','Y14','Y15','Y16','Y17','Y18','Y19','Y20','Y21','Y22','Y23','Y24','Y25','Y26','Y27','Y28','Y29','Y30','Y31','Y32','Y33','Y34','Y35','Y36','Y37','Y38','Y39','Y40','Y41','Y42','Y43','Y44','Y45','Y46','Y47','Y48','Y49','Y50','Y51','Y52','Y53','Y54','Y55','Y56','Y57','Y58','Y59','Y60','Y61','Y62','Y63','Y64','Y65','Y66','Y67','Y68','Y69','Y70','Y71','Y72','Y73','Y74','Y75','Y76','Y77','Y78','Y79','Y80','Y81','Y82','Y83','Y84'))
  and (substr(d.cause_of_death_code_6, 1, 3) is null
    or substr(d.cause_of_death_code_6, 1, 3) not in ('V01','V02','V03','V04','V05','V06','V07','V08','V09','V10','V11','V12','V13','V14','V15','V16','V17','V18','V19','V20','V21','V22','V23','V24','V25','V26','V27','V28','V29','V30','V31','V32','V33','V34','V35','V36','V37','V38','V39','V40','V41','V42','V43','V44','V45','V46','V47','V48','V49','V50','V51','V52','V53','V54','V55','V56','V57','V58','V59','V60','V61','V62','V63','V64','V65','V66','V67','V68','V69','V70','V71','V72','V73','V74','V75','V76','V77','V78','V79','V80','V81','V82','V83','V84','V85','V86','V87','V88','V89','V90','V91','V92','V93','V94','V95','V96','V97','V98','V99','W20','W21','W22','W23','W24','W25','W26','W27','W28','W29','W30','W31','W32','W33','W34','W35','W36','W37','W38','W39','W40','W41','W42','W43','W44','W45','W46','W47','W48','W49','W50','W51','W52','W53','W54','W55','W56','W57','W58','W59','W60','W61','W62','W63','W64','W65','W66','W67','W68','W69','W70','W71','W72','W73','W74','W75','W76','W77','W78','W79','W80','W81','W82','W83','W84','W85','W86','W87','W88','W89','W90','W91','W92','W93','W94','W95','W96','W97','W98','W99','X00','X01','X02','X03','X04','X05','X06','X07','X08','X09','X10','X11','X12','X13','X14','X15','X16','X17','X18','X19','X20','X21','X22','X23','X24','X25','X26','X27','X28','X29','X30','X31','X32','X33','X34','X35','X36','X37','X38','X39','X40','X41','X42','X43','X44','X45','X46','X47','X48','X49','X50','X51','X52','X53','X54','X55','X56','X57','X58','X59','X60','X61','X62','X63','X64','X65','X66','X67','X68','X69','X70','X71','X72','X73','X74','X75','X76','X77','X78','X79','X80','X81','X82','X83','X84','X85','X86','X87','X88','X89','X90','X91','X92','X93','X94','X95','X96','X97','X98','X99','Y00','Y01','Y02','Y03','Y04','Y05','Y06','Y07','Y08','Y09','Y10','Y11','Y12','Y13','Y14','Y15','Y16','Y17','Y18','Y19','Y20','Y21','Y22','Y23','Y24','Y25','Y26','Y27','Y28','Y29','Y30','Y31','Y32','Y33','Y34','Y35','Y36','Y37','Y38','Y39','Y40','Y41','Y42','Y43','Y44','Y45','Y46','Y47','Y48','Y49','Y50','Y51','Y52','Y53','Y54','Y55','Y56','Y57','Y58','Y59','Y60','Y61','Y62','Y63','Y64','Y65','Y66','Y67','Y68','Y69','Y70','Y71','Y72','Y73','Y74','Y75','Y76','Y77','Y78','Y79','Y80','Y81','Y82','Y83','Y84'))
  and (substr(d.cause_of_death_code_7, 1, 3) is null
    or substr(d.cause_of_death_code_7, 1, 3) not in ('V01','V02','V03','V04','V05','V06','V07','V08','V09','V10','V11','V12','V13','V14','V15','V16','V17','V18','V19','V20','V21','V22','V23','V24','V25','V26','V27','V28','V29','V30','V31','V32','V33','V34','V35','V36','V37','V38','V39','V40','V41','V42','V43','V44','V45','V46','V47','V48','V49','V50','V51','V52','V53','V54','V55','V56','V57','V58','V59','V60','V61','V62','V63','V64','V65','V66','V67','V68','V69','V70','V71','V72','V73','V74','V75','V76','V77','V78','V79','V80','V81','V82','V83','V84','V85','V86','V87','V88','V89','V90','V91','V92','V93','V94','V95','V96','V97','V98','V99','W20','W21','W22','W23','W24','W25','W26','W27','W28','W29','W30','W31','W32','W33','W34','W35','W36','W37','W38','W39','W40','W41','W42','W43','W44','W45','W46','W47','W48','W49','W50','W51','W52','W53','W54','W55','W56','W57','W58','W59','W60','W61','W62','W63','W64','W65','W66','W67','W68','W69','W70','W71','W72','W73','W74','W75','W76','W77','W78','W79','W80','W81','W82','W83','W84','W85','W86','W87','W88','W89','W90','W91','W92','W93','W94','W95','W96','W97','W98','W99','X00','X01','X02','X03','X04','X05','X06','X07','X08','X09','X10','X11','X12','X13','X14','X15','X16','X17','X18','X19','X20','X21','X22','X23','X24','X25','X26','X27','X28','X29','X30','X31','X32','X33','X34','X35','X36','X37','X38','X39','X40','X41','X42','X43','X44','X45','X46','X47','X48','X49','X50','X51','X52','X53','X54','X55','X56','X57','X58','X59','X60','X61','X62','X63','X64','X65','X66','X67','X68','X69','X70','X71','X72','X73','X74','X75','X76','X77','X78','X79','X80','X81','X82','X83','X84','X85','X86','X87','X88','X89','X90','X91','X92','X93','X94','X95','X96','X97','X98','X99','Y00','Y01','Y02','Y03','Y04','Y05','Y06','Y07','Y08','Y09','Y10','Y11','Y12','Y13','Y14','Y15','Y16','Y17','Y18','Y19','Y20','Y21','Y22','Y23','Y24','Y25','Y26','Y27','Y28','Y29','Y30','Y31','Y32','Y33','Y34','Y35','Y36','Y37','Y38','Y39','Y40','Y41','Y42','Y43','Y44','Y45','Y46','Y47','Y48','Y49','Y50','Y51','Y52','Y53','Y54','Y55','Y56','Y57','Y58','Y59','Y60','Y61','Y62','Y63','Y64','Y65','Y66','Y67','Y68','Y69','Y70','Y71','Y72','Y73','Y74','Y75','Y76','Y77','Y78','Y79','Y80','Y81','Y82','Y83','Y84'))
  and (substr(d.cause_of_death_code_8, 1, 3) is null
    or substr(d.cause_of_death_code_8, 1, 3) not in ('V01','V02','V03','V04','V05','V06','V07','V08','V09','V10','V11','V12','V13','V14','V15','V16','V17','V18','V19','V20','V21','V22','V23','V24','V25','V26','V27','V28','V29','V30','V31','V32','V33','V34','V35','V36','V37','V38','V39','V40','V41','V42','V43','V44','V45','V46','V47','V48','V49','V50','V51','V52','V53','V54','V55','V56','V57','V58','V59','V60','V61','V62','V63','V64','V65','V66','V67','V68','V69','V70','V71','V72','V73','V74','V75','V76','V77','V78','V79','V80','V81','V82','V83','V84','V85','V86','V87','V88','V89','V90','V91','V92','V93','V94','V95','V96','V97','V98','V99','W20','W21','W22','W23','W24','W25','W26','W27','W28','W29','W30','W31','W32','W33','W34','W35','W36','W37','W38','W39','W40','W41','W42','W43','W44','W45','W46','W47','W48','W49','W50','W51','W52','W53','W54','W55','W56','W57','W58','W59','W60','W61','W62','W63','W64','W65','W66','W67','W68','W69','W70','W71','W72','W73','W74','W75','W76','W77','W78','W79','W80','W81','W82','W83','W84','W85','W86','W87','W88','W89','W90','W91','W92','W93','W94','W95','W96','W97','W98','W99','X00','X01','X02','X03','X04','X05','X06','X07','X08','X09','X10','X11','X12','X13','X14','X15','X16','X17','X18','X19','X20','X21','X22','X23','X24','X25','X26','X27','X28','X29','X30','X31','X32','X33','X34','X35','X36','X37','X38','X39','X40','X41','X42','X43','X44','X45','X46','X47','X48','X49','X50','X51','X52','X53','X54','X55','X56','X57','X58','X59','X60','X61','X62','X63','X64','X65','X66','X67','X68','X69','X70','X71','X72','X73','X74','X75','X76','X77','X78','X79','X80','X81','X82','X83','X84','X85','X86','X87','X88','X89','X90','X91','X92','X93','X94','X95','X96','X97','X98','X99','Y00','Y01','Y02','Y03','Y04','Y05','Y06','Y07','Y08','Y09','Y10','Y11','Y12','Y13','Y14','Y15','Y16','Y17','Y18','Y19','Y20','Y21','Y22','Y23','Y24','Y25','Y26','Y27','Y28','Y29','Y30','Y31','Y32','Y33','Y34','Y35','Y36','Y37','Y38','Y39','Y40','Y41','Y42','Y43','Y44','Y45','Y46','Y47','Y48','Y49','Y50','Y51','Y52','Y53','Y54','Y55','Y56','Y57','Y58','Y59','Y60','Y61','Y62','Y63','Y64','Y65','Y66','Y67','Y68','Y69','Y70','Y71','Y72','Y73','Y74','Y75','Y76','Y77','Y78','Y79','Y80','Y81','Y82','Y83','Y84'))
  and (substr(d.cause_of_death_code_9, 1, 3) is null
    or substr(d.cause_of_death_code_9, 1, 3) not in ('V01','V02','V03','V04','V05','V06','V07','V08','V09','V10','V11','V12','V13','V14','V15','V16','V17','V18','V19','V20','V21','V22','V23','V24','V25','V26','V27','V28','V29','V30','V31','V32','V33','V34','V35','V36','V37','V38','V39','V40','V41','V42','V43','V44','V45','V46','V47','V48','V49','V50','V51','V52','V53','V54','V55','V56','V57','V58','V59','V60','V61','V62','V63','V64','V65','V66','V67','V68','V69','V70','V71','V72','V73','V74','V75','V76','V77','V78','V79','V80','V81','V82','V83','V84','V85','V86','V87','V88','V89','V90','V91','V92','V93','V94','V95','V96','V97','V98','V99','W20','W21','W22','W23','W24','W25','W26','W27','W28','W29','W30','W31','W32','W33','W34','W35','W36','W37','W38','W39','W40','W41','W42','W43','W44','W45','W46','W47','W48','W49','W50','W51','W52','W53','W54','W55','W56','W57','W58','W59','W60','W61','W62','W63','W64','W65','W66','W67','W68','W69','W70','W71','W72','W73','W74','W75','W76','W77','W78','W79','W80','W81','W82','W83','W84','W85','W86','W87','W88','W89','W90','W91','W92','W93','W94','W95','W96','W97','W98','W99','X00','X01','X02','X03','X04','X05','X06','X07','X08','X09','X10','X11','X12','X13','X14','X15','X16','X17','X18','X19','X20','X21','X22','X23','X24','X25','X26','X27','X28','X29','X30','X31','X32','X33','X34','X35','X36','X37','X38','X39','X40','X41','X42','X43','X44','X45','X46','X47','X48','X49','X50','X51','X52','X53','X54','X55','X56','X57','X58','X59','X60','X61','X62','X63','X64','X65','X66','X67','X68','X69','X70','X71','X72','X73','X74','X75','X76','X77','X78','X79','X80','X81','X82','X83','X84','X85','X86','X87','X88','X89','X90','X91','X92','X93','X94','X95','X96','X97','X98','X99','Y00','Y01','Y02','Y03','Y04','Y05','Y06','Y07','Y08','Y09','Y10','Y11','Y12','Y13','Y14','Y15','Y16','Y17','Y18','Y19','Y20','Y21','Y22','Y23','Y24','Y25','Y26','Y27','Y28','Y29','Y30','Y31','Y32','Y33','Y34','Y35','Y36','Y37','Y38','Y39','Y40','Y41','Y42','Y43','Y44','Y45','Y46','Y47','Y48','Y49','Y50','Y51','Y52','Y53','Y54','Y55','Y56','Y57','Y58','Y59','Y60','Y61','Y62','Y63','Y64','Y65','Y66','Y67','Y68','Y69','Y70','Y71','Y72','Y73','Y74','Y75','Y76','Y77','Y78','Y79','Y80','Y81','Y82','Y83','Y84')))
    or (substr(d.underlying_cause_of_death, 1, 3) in ('W00','W01','W02','W03','W04','W05','W06','W07','W08','W09','W10','W11','W12','W13','W14','W15','W16','W17','W18','W19')
    or substr(d.cause_of_death_code_0, 1, 3) in ('W00','W01','W02','W03','W04','W05','W06','W07','W08','W09','W10','W11','W12','W13','W14','W15','W16','W17','W18','W19')
    or substr(d.cause_of_death_code_1, 1, 3) in ('W00','W01','W02','W03','W04','W05','W06','W07','W08','W09','W10','W11','W12','W13','W14','W15','W16','W17','W18','W19')
    or substr(d.cause_of_death_code_2, 1, 3) in ('W00','W01','W02','W03','W04','W05','W06','W07','W08','W09','W10','W11','W12','W13','W14','W15','W16','W17','W18','W19')
    or substr(d.cause_of_death_code_3, 1, 3) in ('W00','W01','W02','W03','W04','W05','W06','W07','W08','W09','W10','W11','W12','W13','W14','W15','W16','W17','W18','W19')
    or substr(d.cause_of_death_code_4, 1, 3) in ('W00','W01','W02','W03','W04','W05','W06','W07','W08','W09','W10','W11','W12','W13','W14','W15','W16','W17','W18','W19')
    or substr(d.cause_of_death_code_5, 1, 3) in ('W00','W01','W02','W03','W04','W05','W06','W07','W08','W09','W10','W11','W12','W13','W14','W15','W16','W17','W18','W19')
    or substr(d.cause_of_death_code_6, 1, 3) in ('W00','W01','W02','W03','W04','W05','W06','W07','W08','W09','W10','W11','W12','W13','W14','W15','W16','W17','W18','W19')
    or substr(d.cause_of_death_code_7, 1, 3) in ('W00','W01','W02','W03','W04','W05','W06','W07','W08','W09','W10','W11','W12','W13','W14','W15','W16','W17','W18','W19')
    or substr(d.cause_of_death_code_8, 1, 3) in ('W00','W01','W02','W03','W04','W05','W06','W07','W08','W09','W10','W11','W12','W13','W14','W15','W16','W17','W18','W19')
    or substr(d.cause_of_death_code_9, 1, 3) in ('W00','W01','W02','W03','W04','W05','W06','W07','W08','W09','W10','W11','W12','W13','W14','W15','W16','W17','W18','W19')))
  and (d.date_of_death between '{extract_start}' and '{extract_end}')
  and d.postcode is not null
order by s.link_no, s.admission_date, s.discharge_date, s.admission, s.discharge, s.uri


