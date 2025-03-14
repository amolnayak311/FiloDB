// Generated from PromQL.g4 by ANTLR 4.9.3
package filodb.prometheus.antlr;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.*;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class PromQLLexer extends Lexer {
	static { RuntimeMetaData.checkVersion("4.9.3", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, T__6=7, T__7=8, NUMBER=9, 
		STRING=10, ADD=11, SUB=12, MUL=13, DIV=14, MOD=15, POW=16, EQ=17, DEQ=18, 
		NE=19, GT=20, LT=21, GE=22, LE=23, RE=24, NRE=25, AND=26, OR=27, UNLESS=28, 
		BY=29, WITHOUT=30, ON=31, IGNORING=32, GROUP_LEFT=33, GROUP_RIGHT=34, 
		OFFSET=35, LIMIT=36, BOOL=37, AGGREGATION_OP=38, DURATION=39, IDENTIFIER=40, 
		IDENTIFIER_EXTENDED=41, WS=42, COMMENT=43;
	public static String[] channelNames = {
		"DEFAULT_TOKEN_CHANNEL", "HIDDEN"
	};

	public static String[] modeNames = {
		"DEFAULT_MODE"
	};

	private static String[] makeRuleNames() {
		return new String[] {
			"T__0", "T__1", "T__2", "T__3", "T__4", "T__5", "T__6", "T__7", "NUMBER", 
			"STRING", "ADD", "SUB", "MUL", "DIV", "MOD", "POW", "EQ", "DEQ", "NE", 
			"GT", "LT", "GE", "LE", "RE", "NRE", "AND", "OR", "UNLESS", "BY", "WITHOUT", 
			"ON", "IGNORING", "GROUP_LEFT", "GROUP_RIGHT", "OFFSET", "LIMIT", "BOOL", 
			"AGGREGATION_OP", "DURATION", "IDENTIFIER", "IDENTIFIER_EXTENDED", "A", 
			"B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", 
			"P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z", "WS", "COMMENT"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
			null, "'('", "')'", "'{'", "'}'", "'['", "']'", "':'", "','", null, null, 
			"'+'", "'-'", "'*'", "'/'", "'%'", "'^'", "'='", "'=='", "'!='", "'>'", 
			"'<'", "'>='", "'<='", "'=~'", "'!~'"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, null, null, null, null, null, null, null, null, "NUMBER", "STRING", 
			"ADD", "SUB", "MUL", "DIV", "MOD", "POW", "EQ", "DEQ", "NE", "GT", "LT", 
			"GE", "LE", "RE", "NRE", "AND", "OR", "UNLESS", "BY", "WITHOUT", "ON", 
			"IGNORING", "GROUP_LEFT", "GROUP_RIGHT", "OFFSET", "LIMIT", "BOOL", "AGGREGATION_OP", 
			"DURATION", "IDENTIFIER", "IDENTIFIER_EXTENDED", "WS", "COMMENT"
		};
	}
	private static final String[] _SYMBOLIC_NAMES = makeSymbolicNames();
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}


	public PromQLLexer(CharStream input) {
		super(input);
		_interp = new LexerATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@Override
	public String getGrammarFileName() { return "PromQL.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public String[] getChannelNames() { return channelNames; }

	@Override
	public String[] getModeNames() { return modeNames; }

	@Override
	public ATN getATN() { return _ATN; }

	public static final String _serializedATN =
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\2-\u0206\b\1\4\2\t"+
		"\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
		"\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
		"\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
		"\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t+\4"+
		",\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63\4\64\t"+
		"\64\4\65\t\65\4\66\t\66\4\67\t\67\48\t8\49\t9\4:\t:\4;\t;\4<\t<\4=\t="+
		"\4>\t>\4?\t?\4@\t@\4A\tA\4B\tB\4C\tC\4D\tD\4E\tE\4F\tF\3\2\3\2\3\3\3\3"+
		"\3\4\3\4\3\5\3\5\3\6\3\6\3\7\3\7\3\b\3\b\3\t\3\t\3\n\7\n\u009f\n\n\f\n"+
		"\16\n\u00a2\13\n\3\n\5\n\u00a5\n\n\3\n\6\n\u00a8\n\n\r\n\16\n\u00a9\3"+
		"\n\3\n\5\n\u00ae\n\n\3\n\6\n\u00b1\n\n\r\n\16\n\u00b2\5\n\u00b5\n\n\3"+
		"\n\6\n\u00b8\n\n\r\n\16\n\u00b9\3\n\3\n\3\n\3\n\6\n\u00c0\n\n\r\n\16\n"+
		"\u00c1\5\n\u00c4\n\n\3\13\3\13\3\13\3\13\7\13\u00ca\n\13\f\13\16\13\u00cd"+
		"\13\13\3\13\3\13\3\13\3\13\3\13\7\13\u00d4\n\13\f\13\16\13\u00d7\13\13"+
		"\3\13\5\13\u00da\n\13\3\f\3\f\3\r\3\r\3\16\3\16\3\17\3\17\3\20\3\20\3"+
		"\21\3\21\3\22\3\22\3\23\3\23\3\23\3\24\3\24\3\24\3\25\3\25\3\26\3\26\3"+
		"\27\3\27\3\27\3\30\3\30\3\30\3\31\3\31\3\31\3\32\3\32\3\32\3\33\3\33\3"+
		"\33\3\33\3\34\3\34\3\34\3\35\3\35\3\35\3\35\3\35\3\35\3\35\3\36\3\36\3"+
		"\36\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3\37\3 \3 \3 \3!\3!\3!\3!\3!\3"+
		"!\3!\3!\3!\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3#\3#\3#\3#\3#"+
		"\3#\3#\3#\3#\3#\3#\3#\3$\3$\3$\3$\3$\3$\3$\3%\3%\3%\3%\3%\3%\3&\3&\3&"+
		"\3&\3&\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'"+
		"\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3"+
		"\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'"+
		"\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3"+
		"\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\3\'\5\'\u01a1\n"+
		"\'\3(\3(\3(\6(\u01a6\n(\r(\16(\u01a7\3(\3(\3(\5(\u01ad\n(\3)\3)\7)\u01b1"+
		"\n)\f)\16)\u01b4\13)\3*\7*\u01b7\n*\f*\16*\u01ba\13*\3*\3*\7*\u01be\n"+
		"*\f*\16*\u01c1\13*\3+\3+\3,\3,\3-\3-\3.\3.\3/\3/\3\60\3\60\3\61\3\61\3"+
		"\62\3\62\3\63\3\63\3\64\3\64\3\65\3\65\3\66\3\66\3\67\3\67\38\38\39\3"+
		"9\3:\3:\3;\3;\3<\3<\3=\3=\3>\3>\3?\3?\3@\3@\3A\3A\3B\3B\3C\3C\3D\3D\3"+
		"E\6E\u01f8\nE\rE\16E\u01f9\3E\3E\3F\3F\7F\u0200\nF\fF\16F\u0203\13F\3"+
		"F\3F\2\2G\3\3\5\4\7\5\t\6\13\7\r\b\17\t\21\n\23\13\25\f\27\r\31\16\33"+
		"\17\35\20\37\21!\22#\23%\24\'\25)\26+\27-\30/\31\61\32\63\33\65\34\67"+
		"\359\36;\37= ?!A\"C#E$G%I&K\'M(O)Q*S+U\2W\2Y\2[\2]\2_\2a\2c\2e\2g\2i\2"+
		"k\2m\2o\2q\2s\2u\2w\2y\2{\2}\2\177\2\u0081\2\u0083\2\u0085\2\u0087\2\u0089"+
		",\u008b-\3\2)\3\2\62;\4\2GGgg\4\2--//\4\2ZZzz\5\2\62;CHch\4\2))^^\4\2"+
		"$$^^\b\2ffjjoouuyy{{\5\2C\\aac|\6\2\62;C\\aac|\4\2<<aa\4\2C\\c|\7\2/\60"+
		"\62<C\\aac|\4\2CCcc\4\2DDdd\4\2EEee\4\2FFff\4\2HHhh\4\2IIii\4\2JJjj\4"+
		"\2KKkk\4\2LLll\4\2MMmm\4\2NNnn\4\2OOoo\4\2PPpp\4\2QQqq\4\2RRrr\4\2SSs"+
		"s\4\2TTtt\4\2UUuu\4\2VVvv\4\2WWww\4\2XXxx\4\2YYyy\4\2[[{{\4\2\\\\||\5"+
		"\2\13\f\17\17\"\"\4\2\f\f\17\17\2\u020d\2\3\3\2\2\2\2\5\3\2\2\2\2\7\3"+
		"\2\2\2\2\t\3\2\2\2\2\13\3\2\2\2\2\r\3\2\2\2\2\17\3\2\2\2\2\21\3\2\2\2"+
		"\2\23\3\2\2\2\2\25\3\2\2\2\2\27\3\2\2\2\2\31\3\2\2\2\2\33\3\2\2\2\2\35"+
		"\3\2\2\2\2\37\3\2\2\2\2!\3\2\2\2\2#\3\2\2\2\2%\3\2\2\2\2\'\3\2\2\2\2)"+
		"\3\2\2\2\2+\3\2\2\2\2-\3\2\2\2\2/\3\2\2\2\2\61\3\2\2\2\2\63\3\2\2\2\2"+
		"\65\3\2\2\2\2\67\3\2\2\2\29\3\2\2\2\2;\3\2\2\2\2=\3\2\2\2\2?\3\2\2\2\2"+
		"A\3\2\2\2\2C\3\2\2\2\2E\3\2\2\2\2G\3\2\2\2\2I\3\2\2\2\2K\3\2\2\2\2M\3"+
		"\2\2\2\2O\3\2\2\2\2Q\3\2\2\2\2S\3\2\2\2\2\u0089\3\2\2\2\2\u008b\3\2\2"+
		"\2\3\u008d\3\2\2\2\5\u008f\3\2\2\2\7\u0091\3\2\2\2\t\u0093\3\2\2\2\13"+
		"\u0095\3\2\2\2\r\u0097\3\2\2\2\17\u0099\3\2\2\2\21\u009b\3\2\2\2\23\u00c3"+
		"\3\2\2\2\25\u00d9\3\2\2\2\27\u00db\3\2\2\2\31\u00dd\3\2\2\2\33\u00df\3"+
		"\2\2\2\35\u00e1\3\2\2\2\37\u00e3\3\2\2\2!\u00e5\3\2\2\2#\u00e7\3\2\2\2"+
		"%\u00e9\3\2\2\2\'\u00ec\3\2\2\2)\u00ef\3\2\2\2+\u00f1\3\2\2\2-\u00f3\3"+
		"\2\2\2/\u00f6\3\2\2\2\61\u00f9\3\2\2\2\63\u00fc\3\2\2\2\65\u00ff\3\2\2"+
		"\2\67\u0103\3\2\2\29\u0106\3\2\2\2;\u010d\3\2\2\2=\u0110\3\2\2\2?\u0118"+
		"\3\2\2\2A\u011b\3\2\2\2C\u0124\3\2\2\2E\u012f\3\2\2\2G\u013b\3\2\2\2I"+
		"\u0142\3\2\2\2K\u0148\3\2\2\2M\u01a0\3\2\2\2O\u01ac\3\2\2\2Q\u01ae\3\2"+
		"\2\2S\u01b8\3\2\2\2U\u01c2\3\2\2\2W\u01c4\3\2\2\2Y\u01c6\3\2\2\2[\u01c8"+
		"\3\2\2\2]\u01ca\3\2\2\2_\u01cc\3\2\2\2a\u01ce\3\2\2\2c\u01d0\3\2\2\2e"+
		"\u01d2\3\2\2\2g\u01d4\3\2\2\2i\u01d6\3\2\2\2k\u01d8\3\2\2\2m\u01da\3\2"+
		"\2\2o\u01dc\3\2\2\2q\u01de\3\2\2\2s\u01e0\3\2\2\2u\u01e2\3\2\2\2w\u01e4"+
		"\3\2\2\2y\u01e6\3\2\2\2{\u01e8\3\2\2\2}\u01ea\3\2\2\2\177\u01ec\3\2\2"+
		"\2\u0081\u01ee\3\2\2\2\u0083\u01f0\3\2\2\2\u0085\u01f2\3\2\2\2\u0087\u01f4"+
		"\3\2\2\2\u0089\u01f7\3\2\2\2\u008b\u01fd\3\2\2\2\u008d\u008e\7*\2\2\u008e"+
		"\4\3\2\2\2\u008f\u0090\7+\2\2\u0090\6\3\2\2\2\u0091\u0092\7}\2\2\u0092"+
		"\b\3\2\2\2\u0093\u0094\7\177\2\2\u0094\n\3\2\2\2\u0095\u0096\7]\2\2\u0096"+
		"\f\3\2\2\2\u0097\u0098\7_\2\2\u0098\16\3\2\2\2\u0099\u009a\7<\2\2\u009a"+
		"\20\3\2\2\2\u009b\u009c\7.\2\2\u009c\22\3\2\2\2\u009d\u009f\t\2\2\2\u009e"+
		"\u009d\3\2\2\2\u009f\u00a2\3\2\2\2\u00a0\u009e\3\2\2\2\u00a0\u00a1\3\2"+
		"\2\2\u00a1\u00a4\3\2\2\2\u00a2\u00a0\3\2\2\2\u00a3\u00a5\7\60\2\2\u00a4"+
		"\u00a3\3\2\2\2\u00a4\u00a5\3\2\2\2\u00a5\u00a7\3\2\2\2\u00a6\u00a8\t\2"+
		"\2\2\u00a7\u00a6\3\2\2\2\u00a8\u00a9\3\2\2\2\u00a9\u00a7\3\2\2\2\u00a9"+
		"\u00aa\3\2\2\2\u00aa\u00b4\3\2\2\2\u00ab\u00ad\t\3\2\2\u00ac\u00ae\t\4"+
		"\2\2\u00ad\u00ac\3\2\2\2\u00ad\u00ae\3\2\2\2\u00ae\u00b0\3\2\2\2\u00af"+
		"\u00b1\t\2\2\2\u00b0\u00af\3\2\2\2\u00b1\u00b2\3\2\2\2\u00b2\u00b0\3\2"+
		"\2\2\u00b2\u00b3\3\2\2\2\u00b3\u00b5\3\2\2\2\u00b4\u00ab\3\2\2\2\u00b4"+
		"\u00b5\3\2\2\2\u00b5\u00c4\3\2\2\2\u00b6\u00b8\t\2\2\2\u00b7\u00b6\3\2"+
		"\2\2\u00b8\u00b9\3\2\2\2\u00b9\u00b7\3\2\2\2\u00b9\u00ba\3\2\2\2\u00ba"+
		"\u00bb\3\2\2\2\u00bb\u00c4\7\60\2\2\u00bc\u00bd\7\62\2\2\u00bd\u00bf\t"+
		"\5\2\2\u00be\u00c0\t\6\2\2\u00bf\u00be\3\2\2\2\u00c0\u00c1\3\2\2\2\u00c1"+
		"\u00bf\3\2\2\2\u00c1\u00c2\3\2\2\2\u00c2\u00c4\3\2\2\2\u00c3\u00a0\3\2"+
		"\2\2\u00c3\u00b7\3\2\2\2\u00c3\u00bc\3\2\2\2\u00c4\24\3\2\2\2\u00c5\u00cb"+
		"\7)\2\2\u00c6\u00ca\n\7\2\2\u00c7\u00c8\7^\2\2\u00c8\u00ca\13\2\2\2\u00c9"+
		"\u00c6\3\2\2\2\u00c9\u00c7\3\2\2\2\u00ca\u00cd\3\2\2\2\u00cb\u00c9\3\2"+
		"\2\2\u00cb\u00cc\3\2\2\2\u00cc\u00ce\3\2\2\2\u00cd\u00cb\3\2\2\2\u00ce"+
		"\u00da\7)\2\2\u00cf\u00d5\7$\2\2\u00d0\u00d4\n\b\2\2\u00d1\u00d2\7^\2"+
		"\2\u00d2\u00d4\13\2\2\2\u00d3\u00d0\3\2\2\2\u00d3\u00d1\3\2\2\2\u00d4"+
		"\u00d7\3\2\2\2\u00d5\u00d3\3\2\2\2\u00d5\u00d6\3\2\2\2\u00d6\u00d8\3\2"+
		"\2\2\u00d7\u00d5\3\2\2\2\u00d8\u00da\7$\2\2\u00d9\u00c5\3\2\2\2\u00d9"+
		"\u00cf\3\2\2\2\u00da\26\3\2\2\2\u00db\u00dc\7-\2\2\u00dc\30\3\2\2\2\u00dd"+
		"\u00de\7/\2\2\u00de\32\3\2\2\2\u00df\u00e0\7,\2\2\u00e0\34\3\2\2\2\u00e1"+
		"\u00e2\7\61\2\2\u00e2\36\3\2\2\2\u00e3\u00e4\7\'\2\2\u00e4 \3\2\2\2\u00e5"+
		"\u00e6\7`\2\2\u00e6\"\3\2\2\2\u00e7\u00e8\7?\2\2\u00e8$\3\2\2\2\u00e9"+
		"\u00ea\7?\2\2\u00ea\u00eb\7?\2\2\u00eb&\3\2\2\2\u00ec\u00ed\7#\2\2\u00ed"+
		"\u00ee\7?\2\2\u00ee(\3\2\2\2\u00ef\u00f0\7@\2\2\u00f0*\3\2\2\2\u00f1\u00f2"+
		"\7>\2\2\u00f2,\3\2\2\2\u00f3\u00f4\7@\2\2\u00f4\u00f5\7?\2\2\u00f5.\3"+
		"\2\2\2\u00f6\u00f7\7>\2\2\u00f7\u00f8\7?\2\2\u00f8\60\3\2\2\2\u00f9\u00fa"+
		"\7?\2\2\u00fa\u00fb\7\u0080\2\2\u00fb\62\3\2\2\2\u00fc\u00fd\7#\2\2\u00fd"+
		"\u00fe\7\u0080\2\2\u00fe\64\3\2\2\2\u00ff\u0100\5U+\2\u0100\u0101\5o8"+
		"\2\u0101\u0102\5[.\2\u0102\66\3\2\2\2\u0103\u0104\5q9\2\u0104\u0105\5"+
		"w<\2\u01058\3\2\2\2\u0106\u0107\5}?\2\u0107\u0108\5o8\2\u0108\u0109\5"+
		"k\66\2\u0109\u010a\5]/\2\u010a\u010b\5y=\2\u010b\u010c\5y=\2\u010c:\3"+
		"\2\2\2\u010d\u010e\5W,\2\u010e\u010f\5\u0085C\2\u010f<\3\2\2\2\u0110\u0111"+
		"\5\u0081A\2\u0111\u0112\5e\63\2\u0112\u0113\5{>\2\u0113\u0114\5c\62\2"+
		"\u0114\u0115\5q9\2\u0115\u0116\5}?\2\u0116\u0117\5{>\2\u0117>\3\2\2\2"+
		"\u0118\u0119\5q9\2\u0119\u011a\5o8\2\u011a@\3\2\2\2\u011b\u011c\5e\63"+
		"\2\u011c\u011d\5a\61\2\u011d\u011e\5o8\2\u011e\u011f\5q9\2\u011f\u0120"+
		"\5w<\2\u0120\u0121\5e\63\2\u0121\u0122\5o8\2\u0122\u0123\5a\61\2\u0123"+
		"B\3\2\2\2\u0124\u0125\5a\61\2\u0125\u0126\5w<\2\u0126\u0127\5q9\2\u0127"+
		"\u0128\5}?\2\u0128\u0129\5s:\2\u0129\u012a\7a\2\2\u012a\u012b\5k\66\2"+
		"\u012b\u012c\5]/\2\u012c\u012d\5_\60\2\u012d\u012e\5{>\2\u012eD\3\2\2"+
		"\2\u012f\u0130\5a\61\2\u0130\u0131\5w<\2\u0131\u0132\5q9\2\u0132\u0133"+
		"\5}?\2\u0133\u0134\5s:\2\u0134\u0135\7a\2\2\u0135\u0136\5w<\2\u0136\u0137"+
		"\5e\63\2\u0137\u0138\5a\61\2\u0138\u0139\5c\62\2\u0139\u013a\5{>\2\u013a"+
		"F\3\2\2\2\u013b\u013c\5q9\2\u013c\u013d\5_\60\2\u013d\u013e\5_\60\2\u013e"+
		"\u013f\5y=\2\u013f\u0140\5]/\2\u0140\u0141\5{>\2\u0141H\3\2\2\2\u0142"+
		"\u0143\5k\66\2\u0143\u0144\5e\63\2\u0144\u0145\5m\67\2\u0145\u0146\5e"+
		"\63\2\u0146\u0147\5{>\2\u0147J\3\2\2\2\u0148\u0149\5W,\2\u0149\u014a\5"+
		"q9\2\u014a\u014b\5q9\2\u014b\u014c\5k\66\2\u014cL\3\2\2\2\u014d\u014e"+
		"\5y=\2\u014e\u014f\5}?\2\u014f\u0150\5m\67\2\u0150\u01a1\3\2\2\2\u0151"+
		"\u0152\5m\67\2\u0152\u0153\5e\63\2\u0153\u0154\5o8\2\u0154\u01a1\3\2\2"+
		"\2\u0155\u0156\5m\67\2\u0156\u0157\5U+\2\u0157\u0158\5\u0083B\2\u0158"+
		"\u01a1\3\2\2\2\u0159\u015a\5U+\2\u015a\u015b\5\177@\2\u015b\u015c\5a\61"+
		"\2\u015c\u01a1\3\2\2\2\u015d\u015e\5a\61\2\u015e\u015f\5w<\2\u015f\u0160"+
		"\5q9\2\u0160\u0161\5}?\2\u0161\u0162\5s:\2\u0162\u01a1\3\2\2\2\u0163\u0164"+
		"\5y=\2\u0164\u0165\5{>\2\u0165\u0166\5[.\2\u0166\u0167\5[.\2\u0167\u0168"+
		"\5]/\2\u0168\u0169\5\177@\2\u0169\u01a1\3\2\2\2\u016a\u016b\5y=\2\u016b"+
		"\u016c\5{>\2\u016c\u016d\5[.\2\u016d\u016e\5\177@\2\u016e\u016f\5U+\2"+
		"\u016f\u0170\5w<\2\u0170\u01a1\3\2\2\2\u0171\u0172\5Y-\2\u0172\u0173\5"+
		"q9\2\u0173\u0174\5}?\2\u0174\u0175\5o8\2\u0175\u0176\5{>\2\u0176\u01a1"+
		"\3\2\2\2\u0177\u0178\5Y-\2\u0178\u0179\5q9\2\u0179\u017a\5}?\2\u017a\u017b"+
		"\5o8\2\u017b\u017c\5{>\2\u017c\u017d\7a\2\2\u017d\u017e\5\177@\2\u017e"+
		"\u017f\5U+\2\u017f\u0180\5k\66\2\u0180\u0181\5}?\2\u0181\u0182\5]/\2\u0182"+
		"\u0183\5y=\2\u0183\u01a1\3\2\2\2\u0184\u0185\5W,\2\u0185\u0186\5q9\2\u0186"+
		"\u0187\5{>\2\u0187\u0188\5{>\2\u0188\u0189\5q9\2\u0189\u018a\5m\67\2\u018a"+
		"\u018b\5i\65\2\u018b\u01a1\3\2\2\2\u018c\u018d\5{>\2\u018d\u018e\5q9\2"+
		"\u018e\u018f\5s:\2\u018f\u0190\5i\65\2\u0190\u01a1\3\2\2\2\u0191\u0192"+
		"\5u;\2\u0192\u0193\5}?\2\u0193\u0194\5U+\2\u0194\u0195\5o8\2\u0195\u0196"+
		"\5{>\2\u0196\u0197\5e\63\2\u0197\u0198\5k\66\2\u0198\u0199\5]/\2\u0199"+
		"\u01a1\3\2\2\2\u019a\u019b\5a\61\2\u019b\u019c\5w<\2\u019c\u019d\5q9\2"+
		"\u019d\u019e\5}?\2\u019e\u019f\5s:\2\u019f\u01a1\3\2\2\2\u01a0\u014d\3"+
		"\2\2\2\u01a0\u0151\3\2\2\2\u01a0\u0155\3\2\2\2\u01a0\u0159\3\2\2\2\u01a0"+
		"\u015d\3\2\2\2\u01a0\u0163\3\2\2\2\u01a0\u016a\3\2\2\2\u01a0\u0171\3\2"+
		"\2\2\u01a0\u0177\3\2\2\2\u01a0\u0184\3\2\2\2\u01a0\u018c\3\2\2\2\u01a0"+
		"\u0191\3\2\2\2\u01a0\u019a\3\2\2\2\u01a1N\3\2\2\2\u01a2\u01a3\5\23\n\2"+
		"\u01a3\u01a4\t\t\2\2\u01a4\u01a6\3\2\2\2\u01a5\u01a2\3\2\2\2\u01a6\u01a7"+
		"\3\2\2\2\u01a7\u01a5\3\2\2\2\u01a7\u01a8\3\2\2\2\u01a8\u01ad\3\2\2\2\u01a9"+
		"\u01aa\5\23\n\2\u01aa\u01ab\7k\2\2\u01ab\u01ad\3\2\2\2\u01ac\u01a5\3\2"+
		"\2\2\u01ac\u01a9\3\2\2\2\u01adP\3\2\2\2\u01ae\u01b2\t\n\2\2\u01af\u01b1"+
		"\t\13\2\2\u01b0\u01af\3\2\2\2\u01b1\u01b4\3\2\2\2\u01b2\u01b0\3\2\2\2"+
		"\u01b2\u01b3\3\2\2\2\u01b3R\3\2\2\2\u01b4\u01b2\3\2\2\2\u01b5\u01b7\t"+
		"\f\2\2\u01b6\u01b5\3\2\2\2\u01b7\u01ba\3\2\2\2\u01b8\u01b6\3\2\2\2\u01b8"+
		"\u01b9\3\2\2\2\u01b9\u01bb\3\2\2\2\u01ba\u01b8\3\2\2\2\u01bb\u01bf\t\r"+
		"\2\2\u01bc\u01be\t\16\2\2\u01bd\u01bc\3\2\2\2\u01be\u01c1\3\2\2\2\u01bf"+
		"\u01bd\3\2\2\2\u01bf\u01c0\3\2\2\2\u01c0T\3\2\2\2\u01c1\u01bf\3\2\2\2"+
		"\u01c2\u01c3\t\17\2\2\u01c3V\3\2\2\2\u01c4\u01c5\t\20\2\2\u01c5X\3\2\2"+
		"\2\u01c6\u01c7\t\21\2\2\u01c7Z\3\2\2\2\u01c8\u01c9\t\22\2\2\u01c9\\\3"+
		"\2\2\2\u01ca\u01cb\t\3\2\2\u01cb^\3\2\2\2\u01cc\u01cd\t\23\2\2\u01cd`"+
		"\3\2\2\2\u01ce\u01cf\t\24\2\2\u01cfb\3\2\2\2\u01d0\u01d1\t\25\2\2\u01d1"+
		"d\3\2\2\2\u01d2\u01d3\t\26\2\2\u01d3f\3\2\2\2\u01d4\u01d5\t\27\2\2\u01d5"+
		"h\3\2\2\2\u01d6\u01d7\t\30\2\2\u01d7j\3\2\2\2\u01d8\u01d9\t\31\2\2\u01d9"+
		"l\3\2\2\2\u01da\u01db\t\32\2\2\u01dbn\3\2\2\2\u01dc\u01dd\t\33\2\2\u01dd"+
		"p\3\2\2\2\u01de\u01df\t\34\2\2\u01dfr\3\2\2\2\u01e0\u01e1\t\35\2\2\u01e1"+
		"t\3\2\2\2\u01e2\u01e3\t\36\2\2\u01e3v\3\2\2\2\u01e4\u01e5\t\37\2\2\u01e5"+
		"x\3\2\2\2\u01e6\u01e7\t \2\2\u01e7z\3\2\2\2\u01e8\u01e9\t!\2\2\u01e9|"+
		"\3\2\2\2\u01ea\u01eb\t\"\2\2\u01eb~\3\2\2\2\u01ec\u01ed\t#\2\2\u01ed\u0080"+
		"\3\2\2\2\u01ee\u01ef\t$\2\2\u01ef\u0082\3\2\2\2\u01f0\u01f1\t\5\2\2\u01f1"+
		"\u0084\3\2\2\2\u01f2\u01f3\t%\2\2\u01f3\u0086\3\2\2\2\u01f4\u01f5\t&\2"+
		"\2\u01f5\u0088\3\2\2\2\u01f6\u01f8\t\'\2\2\u01f7\u01f6\3\2\2\2\u01f8\u01f9"+
		"\3\2\2\2\u01f9\u01f7\3\2\2\2\u01f9\u01fa\3\2\2\2\u01fa\u01fb\3\2\2\2\u01fb"+
		"\u01fc\bE\2\2\u01fc\u008a\3\2\2\2\u01fd\u0201\7%\2\2\u01fe\u0200\n(\2"+
		"\2\u01ff\u01fe\3\2\2\2\u0200\u0203\3\2\2\2\u0201\u01ff\3\2\2\2\u0201\u0202"+
		"\3\2\2\2\u0202\u0204\3\2\2\2\u0203\u0201\3\2\2\2\u0204\u0205\bF\2\2\u0205"+
		"\u008c\3\2\2\2\31\2\u00a0\u00a4\u00a9\u00ad\u00b2\u00b4\u00b9\u00c1\u00c3"+
		"\u00c9\u00cb\u00d3\u00d5\u00d9\u01a0\u01a7\u01ac\u01b2\u01b8\u01bf\u01f9"+
		"\u0201\3\b\2\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}