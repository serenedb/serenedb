-- 256-word common English vocabulary; reused by english/ngram cases.
-- Materialised as a real table so generators can JOIN deterministically.
DROP TABLE IF EXISTS sweep_vocab;
CREATE TABLE sweep_vocab (idx INTEGER PRIMARY KEY, word VARCHAR);
WITH words AS (SELECT unnest([
  'the','of','and','to','a','in','for','is','on','that','by','this','with','i','you','it',
  'not','or','be','are','from','at','as','your','all','have','new','more','an','was','we','will',
  'home','can','us','about','if','page','my','has','search','free','but','our','one','other','do','no',
  'information','time','they','site','he','up','may','what','which','their','news','out','use','any','there','see',
  'only','so','his','when','contact','here','business','who','web','also','now','help','get','pm','view','online',
  'first','am','been','would','how','were','me','some','these','click','its','like','service','than','find','price',
  'date','back','top','people','had','list','name','just','over','state','year','day','into','email','two','health',
  'world','re','next','used','go','work','last','most','products','music','buy','data','make','them','should','product',
  'system','post','her','city','add','policy','number','such','please','available','copyright','support','message','after','best','software',
  'then','jan','good','video','well','where','info','rights','public','books','high','school','through','each','links','she',
  'review','years','order','very','privacy','book','items','company','read','group','sex','need','many','user','said','de',
  'does','set','under','general','research','university','january','mail','full','map','reviews','program','life','know','games','way',
  'days','management','part','could','great','united','hotel','real','item','international','center','must','store','travel','comments','made',
  'development','report','off','member','details','line','terms','before','hotels','did','send','right','type','because','local','those',
  'using','results','office','education','national','car','design','take','posted','internet','address','community','within','states','area','want',
  'phone','dvd','shipping','reserved','subject','between','forum','family','long','based','code','show','even','black','check','special'
]) AS word)
INSERT INTO sweep_vocab SELECT (row_number() OVER (ORDER BY word)) - 1, word FROM words;
