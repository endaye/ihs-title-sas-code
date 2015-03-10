option compress =yes;
%LET txtds = ./;
libname f './';

*** read dictionary ***;
data f.dict;
infile "./Dict_TITLE_New.txt" dsd truncover firstobs=2 dlm='09'x lrecl=99999999;
input Level: $1. Field: $50. Length: best12. Variable: $24. var_idx: best12.;
run;
*** assign macro variables for looping in macro function ***;
data _NULL_;
set f.dict(where=(level^=''));
call symputx("maxvar", _N_);
if level='H' then do; call symputx("maxh", var_idx); end;
if level='R' then do; call symputx("maxr", var_idx); end;
if level='I' then do; call symputx("maxi", var_idx); end;
if level='M' then do; call symputx("maxm", var_idx); end;
if level='P' then do; call symputx("maxp", var_idx); end;
if level='A' then do; call symputx("maxa", var_idx); end;
if level='L' then do; call symputx("maxl", var_idx); end;
if level='N' then do; call symputx("maxn", var_idx); end;
if level='T' then do; call symputx("maxt", var_idx); end;
call symput(cats("var",_N_), compress(Variable));
call symput(cats("tmpvar",_N_), cats(lowcase(level),"var",var_idx));
call symput(cats("fmt",_N_), cats("$",Length,"."));
run;

%put ~~~ maxvar: &maxvar. ~~~;
%put ~~~ maxh: &maxh. ~~~;
%put ~~~ maxr: &maxr. ~~~;
%put ~~~ maxi: &maxi. ~~~;
%put ~~~ maxm: &maxm. ~~~;
%put ~~~ maxp: &maxp. ~~~;
%put ~~~ maxa: &maxa. ~~~;
%put ~~~ maxl: &maxl. ~~~;
%put ~~~ maxn: &maxn. ~~~;
%put ~~~ maxt: &maxt. ~~~;




%macro data_transpose(data_in, data_out, var_id, var_keep=_ALL_, var_idx=);
*** input dataset data_in, output dataset data_out, var_id as unique ID for transposed data ***;
*** default var_keep keeps all the variables, default blank var_idx does not specify variable of sort order ***;
* sort data_in by specific order if no var_idx;

proc sort data=&data_in(keep = &var_id. &var_keep. ) out=tmpds_0; by &var_id.; run;
*** 初始化数值;
%let tmpnobs = %eval(0);
data _NULL_; set tmpds_0; call symputx('tmpnobs',_N_); run;
*** tmpnobs = data_in总行数;
%put tmpnobs = &tmpnobs; 

*** var_id = Doc_Identifier;
%if &var_idx= %then %do;
	data tmpds_1; set tmpds_0;
	new_id = catx('|', OF &var_id.);
	if new_id ^= lag(new_id) then tmp_idx=1;
	else tmp_idx ++ 1;
	*drop new_id;
	run;
	data f.&data_in._tmpds_1; set tmpds_1; run;
%end;
%else %do;
	proc sort data=tmpds_0 nodupkey; by &var_id. &var_idx.; run;
	data tmpds_1; set tmpds_0; tmp_idx = &var_idx.; drop &var_idx.; run;
%end;

proc contents data=tmpds_1(drop = tmp_idx &var_id.) noprint out=tmpds_ctnt; run;
data _NULL_; set tmpds_ctnt; call symputx('tmpmaxv', _N_); call symput(cats('tmpv',_N_), compress(Name)); run;
%put ~~~~~~ tmpmaxv = &tmpmaxv ~~~~~~;

%if &tmpnobs = 0 %then %do;
	data &data_out;
	set &data_in;
	%do i = 1 %to &tmpmaxv;
		rename &&tmpv&i = %sysfunc(cats(%substr(&&tmpv&i,1,1),1,%substr(&&tmpv&i,2,%length(&&tmpv&i)-1)));
	%end;
	run;
%end;

%else %do;
	proc means data=tmpds_1 noprint; var tmp_idx; output out=tmpds_max_idx max=max; run;
	data _NULL_; set tmpds_max_idx; call symputx('tmpmaxidx', max); run;
	%put ~~~~~~ tmpmaxidx = &tmpmaxidx ~~~~~~;

	data tmpds_multi_dsname;
	set tmpds_max_idx;
	%do i = 1 %to &tmpmaxidx;
		format dsname_&i $10.; dsname_&i = cats('tmp_',&i);
	%end;
	call symput('dsname_all', catx(' ', OF dsname_1 - dsname_&tmpmaxidx.));
	run;

	data &dsname_all;
	set tmpds_1;
	%do i = 1 %to &tmpmaxidx;
		if tmp_idx = &i then output tmp_&i ;
	%end;
	run;

	%do i = 1 %to &tmpmaxidx;
		data tmp_&i;
		set tmp_&i;
		%do j = 1 %to &tmpmaxv;
			rename &&tmpv&j = %sysfunc(cats(%substr(&&tmpv&j,1,1),&i,%substr(&&tmpv&j,2,%length(&&tmpv&j)-1)));
		%end;
		drop tmp_idx;
	%end;

	data &data_out;
	merge tmp_1 - tmp_&tmpmaxidx;
	by &var_id.;
	run;
%end;

%mend data_transpose;


%macro import_single(filein, fileout=, default_over=truncover, default_dlm='7C'x, default_maxlen=6000);
	*** set default output dataset name ***;
	%if &fileout= %then %do;
		%let fileout = %sysfunc(cats(sasds., %scan(%scan(&filein.,-1,/),1,.)));
	%end;

	*** read nobs from Batch Head ***;
	data ds_0;
	infile "&filein." dsd &default_over. lrecl=99999999 dlm=&default_dlm. obs=1;
	input level: $1. Doc_Identifier: $1. FIPS: $5. Batch_Date: $8. Rec_Count: $9. Time_From: $23. Time_To: $23.;
	call symputx("obsnum", rec_count*1-1);
	call symput('dt_batch', Batch_Date);
	run;
	%put ~~~ Obs Number from Batch Header: &obsnum ~~~;


	*** clean dirty value that splits into multiple lines ***;
	data ds_1;
	infile "&filein." &default_over. lrecl=99999999 firstobs=2;
	input raw_all $&default_maxlen..;
	*** 四个变量: raw_all原始数据（默认最大长度6000）, obs_tmp编号, level_tmp种类, flag_tmp标记成一块一块的;
	raw_all = compress(raw_all,&default_dlm.,'kw');
	obs_tmp = _N_;
	level_tmp = substr(raw_all,1,1);
	if (level_tmp='H' | level_tmp='R' | level_tmp='I' | level_tmp='M' | level_tmp='P' | level_tmp='A' 
		| level_tmp='L' | level_tmp='N' | level_tmp='T') & substr(raw_all,2,1)=&default_dlm. then flag_tmp=0;
	else flag_tmp++1;
	run;

	data f.ds_1;
	set ds_1;
	run;
	*** Variable, max,;
	proc means data=ds_1 noprint; var flag_tmp; output out=ds_tmp max=max; run;
	data f.ds_tmp; set ds_tmp; run;
	data _NULL_;
	set ds_tmp;
	call symputx('max_flag',max);
	run;
	%put ~~~ max_flag = &max_flag ~~~;

	%if &max_flag. >= 1 %then %do;
		%do i = 0 %to &max_flag.;
			data ds_tmp&i.;
			set ds_1(where=(flag_tmp=&i.));
			id_tmp = obs_tmp - flag_tmp;
			keep raw_all id_tmp;
			rename raw_all = raw_&i.;
			run;
		%end;
		
		data ds_2;
		merge ds_tmp0 - ds_tmp&max_flag.;
		by id_tmp;
		format raw_all $&default_maxlen..;
		raw_all = cats(OF raw_0 - raw_&max_flag.);
		keep raw_all;
		run;
		data f.ds_2; set ds_2; run;
		/*
		data ds_2_dirty;
		merge ds_tmp1 - ds_tmp&max_flag.;
		by id_tmp;
		format raw_all $&default_maxlen.. id_tmp;
		raw_all = cats(OF raw_1 - raw_&max_flag.);
		keep raw_all id_tmp;
		run;
		data f.ds_2_dirty2;
		set ds_2_dirty;
		a = index(raw_all,'|');
		run;
		*/
	%end;
	%else %do;
		data ds_2; set ds_1; keep raw_all; run;
	%end;

	*** read data with cleaned string format from dictionary ***;
	data ds_3;
	set ds_2;
	%do i = 1 %to &maxvar.;
		format &&tmpvar&i &&fmt&i;
	%end;
	format level $1.;
	level = scan(raw_all,1,&default_dlm.,'m');
	if level='H' then do; 
	%do i = 1 %to &maxh.; hvar&i. = scan(raw_all,1+&i.,&default_dlm.,'m'); %end; end;
	else if level='R' then do; %do i = 1 %to &maxr.; rvar&i. = scan(raw_all,1+&i.,&default_dlm.,'m'); %end; end;
	else if level='I' then do; %do i = 1 %to &maxi.; ivar&i. = scan(raw_all,1+&i.,&default_dlm.,'m'); %end; end;
	else if level='M' then do; %do i = 1 %to &maxm.; mvar&i. = scan(raw_all,1+&i.,&default_dlm.,'m'); %end; end;
	else if level='P' then do; %do i = 1 %to &maxp.; pvar&i. = scan(raw_all,1+&i.,&default_dlm.,'m'); %end; end;
	else if level='A' then do; %do i = 1 %to &maxa.; avar&i. = scan(raw_all,1+&i.,&default_dlm.,'m'); %end; end;
	else if level='L' then do; %do i = 1 %to &maxl.; lvar&i. = scan(raw_all,1+&i.,&default_dlm.,'m'); %end; end;
	else if level='N' then do; %do i = 1 %to &maxn.; nvar&i. = scan(raw_all,1+&i.,&default_dlm.,'m'); %end; end;
	else if level='T' then do; %do i = 1 %to &maxt.; tvar&i. = scan(raw_all,1+&i.,&default_dlm.,'m'); %end; end;
	else do; put 'ERROR: Unknown Level Type ' level 'in Obs: ' _N_; end;
	if level^='I' & level^='M' then indi_idx=0;
	else if level='I' then indi_idx++1;
	else indi_idx++0;
	%do i = 1 %to &maxvar.;
		rename &&tmpvar&i = &&var&i;
	%end;
	call symputx("maxobs", _N_);
	drop raw_all;
	run;
	data f.ds_3; set ds_3; run;
	%put ~~~ Max Obs Read from File: &maxobs ~~~;


	*** compare nobs with Batch Head ***;
	%if &obsnum^=&maxobs %then %do;
		%put "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~";
		%put "ERROR: Observations Read from TXT File NOT Equal to Number Specified in Batch Header";
		%put "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~";
	%end;


	*** split data into different level types sub datasets ***;
	data ds_h(keep=H_:) ds_r(keep=R_:) ds_i(keep=indi_idx I_:) ds_m(keep=indi_idx M_:)
		ds_p(keep=P_:) ds_a(keep=A_:) ds_l(keep=L_:) ds_n(keep=N_:) ds_t(keep=T_:);
	set ds_3;
	if level='H' then output ds_h;
	else if level='R' then output ds_r;
	else if level='I' then output ds_i;
	else if level='M' then output ds_m;
	else if level='P' then output ds_p;
	else if level='A' then output ds_a;
	else if level='L' then output ds_l;
	else if level='N' then output ds_n;
	else if level='T' then output ds_t;
	else do; 
		put 'ERROR: Unknown Level Type ' level 'in Obs: ' _N_; 
	end;
	run;
	/*
	data f.ds_h; set ds_h; run;
	data f.ds_r; set ds_r; run;
	data f.ds_i; set ds_i; run;
	data f.ds_m; set ds_m; run;
	data f.ds_p; set ds_p; run;
	data f.ds_a; set ds_a; run;
	data f.ds_l; set ds_l; run;
	data f.ds_n; set ds_n; run;
	data f.ds_t; set ds_t; run;
	*/

	*** combine I & M level ***;
	proc sort data=ds_i nodupkey; by I_Doc_Identifier indi_idx; run;
	proc sort data=ds_m nodupkey; by M_Doc_Identifier indi_idx; run;
	data ds_im;
	merge ds_i(in=a rename=(I_Doc_Identifier=Doc_Identifier)) ds_m(in=b rename=(M_Doc_Identifier=Doc_Identifier));
	by Doc_Identifier indi_idx;
	run;


	*** transpose IM, R, P, A, N, & T level ***;
	
	data f.ds_h; set ds_h; run;
	data f.ds_r; set ds_r; run;
	data f.ds_im; set ds_im; run;
	data f.ds_p; set ds_p; run;
	data f.ds_a; set ds_a; run;
	data f.ds_l; set ds_l; run;
	data f.ds_n; set ds_n; run;
	data f.ds_t; set ds_t; run;
	%data_transpose(ds_im, ds_im_1, var_id=Doc_Identifier, var_idx=indi_idx);
	%data_transpose(ds_r, ds_r_1, var_id=R_Doc_Identifier);
	%data_transpose(ds_p, ds_p_1, var_id=P_Doc_Identifier);
	%data_transpose(ds_a, ds_a_1, var_id=A_Doc_Identifier);
	%data_transpose(ds_n, ds_n_1, var_id=N_Doc_Identifier);
	%data_transpose(ds_t, ds_t_1, var_id=T_Doc_Identifier);
	
	proc sort data=ds_h nodupkey; by H_Doc_Identifier; run;
	proc sort data=ds_l; by L_Doc_Identifier; run;
	proc sort data=ds_r_1 nodupkey; by R_Doc_Identifier; run;
	proc sort data=ds_im_1 nodupkey; by Doc_Identifier; run;
	proc sort data=ds_p_1 nodupkey; by P_Doc_Identifier; run;
	proc sort data=ds_a_1 nodupkey; by A_Doc_Identifier; run;
	proc sort data=ds_n_1 nodupkey; by N_Doc_Identifier; run;
	proc sort data=ds_t_1 nodupkey; by T_Doc_Identifier; run;
	
	
	data f.ds_h_1; set ds_h_1; run;
	data f.ds_r_1; set ds_r_1; run;
	data f.ds_im_1; set ds_im_1; run;
	data f.ds_p_1; set ds_p_1; run;
	data f.ds_a_1; set ds_a_1; run;
	data f.ds_l_1; set ds_l_1; run;
	data f.ds_n_1; set ds_n_1; run;
	data f.ds_t_1; set ds_t_1; run;


%mend import_single;
%import_single(&txtds.DPL_IL_Cook_20150302.txt);