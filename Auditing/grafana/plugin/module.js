/*! For license information please see module.js.LICENSE.txt */
define(["@grafana/data","react","@grafana/ui"],(e,r,t)=>(()=>{"use strict";var n={20(e,r,t){var n=t(959),a=Symbol.for("react.element"),s=Symbol.for("react.fragment"),i=Object.prototype.hasOwnProperty,o=n.__SECRET_INTERNALS_DO_NOT_USE_OR_YOU_WILL_BE_FIRED.ReactCurrentOwner,c={key:!0,ref:!0,__self:!0,__source:!0};function l(e,r,t){var n,s={},l=null,d=null;for(n in void 0!==t&&(l=""+t),void 0!==r.key&&(l=""+r.key),void 0!==r.ref&&(d=r.ref),r)i.call(r,n)&&!c.hasOwnProperty(n)&&(s[n]=r[n]);if(e&&e.defaultProps)for(n in r=e.defaultProps)void 0===s[n]&&(s[n]=r[n]);return{$$typeof:a,type:e,key:l,ref:d,props:s,_owner:o.current}}r.Fragment=s,r.jsx=l,r.jsxs=l},848(e,r,t){e.exports=t(20)},781(r){r.exports=e},7(e){e.exports=t},959(e){e.exports=r}},a={};function s(e){var r=a[e];if(void 0!==r)return r.exports;var t=a[e]={exports:{}};return n[e](t,t.exports,s),t.exports}s.d=(e,r)=>{for(var t in r)s.o(r,t)&&!s.o(e,t)&&Object.defineProperty(e,t,{enumerable:!0,get:r[t]})},s.o=(e,r)=>Object.prototype.hasOwnProperty.call(e,r),s.r=e=>{"undefined"!=typeof Symbol&&Symbol.toStringTag&&Object.defineProperty(e,Symbol.toStringTag,{value:"Module"}),Object.defineProperty(e,"__esModule",{value:!0})};var i={};s.r(i),s.d(i,{plugin:()=>ze});var o=s(781),c=s(848),l=s(959),d=s(7),u=function(){function e(e){var r=this;this._insertTag=function(e){var t;t=0===r.tags.length?r.insertionPoint?r.insertionPoint.nextSibling:r.prepend?r.container.firstChild:r.before:r.tags[r.tags.length-1].nextSibling,r.container.insertBefore(e,t),r.tags.push(e)},this.isSpeedy=void 0===e.speedy||e.speedy,this.tags=[],this.ctr=0,this.nonce=e.nonce,this.key=e.key,this.container=e.container,this.prepend=e.prepend,this.insertionPoint=e.insertionPoint,this.before=null}var r=e.prototype;return r.hydrate=function(e){e.forEach(this._insertTag)},r.insert=function(e){this.ctr%(this.isSpeedy?65e3:1)==0&&this._insertTag(function(e){var r=document.createElement("style");return r.setAttribute("data-emotion",e.key),void 0!==e.nonce&&r.setAttribute("nonce",e.nonce),r.appendChild(document.createTextNode("")),r.setAttribute("data-s",""),r}(this));var r=this.tags[this.tags.length-1];if(this.isSpeedy){var t=function(e){if(e.sheet)return e.sheet;for(var r=0;r<document.styleSheets.length;r++)if(document.styleSheets[r].ownerNode===e)return document.styleSheets[r]}(r);try{t.insertRule(e,t.cssRules.length)}catch(e){}}else r.appendChild(document.createTextNode(e));this.ctr++},r.flush=function(){this.tags.forEach(function(e){var r;return null==(r=e.parentNode)?void 0:r.removeChild(e)}),this.tags=[],this.ctr=0},e}(),p=Math.abs,f=String.fromCharCode,h=Object.assign;function x(e){return e.trim()}function g(e,r,t){return e.replace(r,t)}function m(e,r){return e.indexOf(r)}function v(e,r){return 0|e.charCodeAt(r)}function b(e,r,t){return e.slice(r,t)}function y(e){return e.length}function w(e){return e.length}function k(e,r){return r.push(e),e}var j=1,S=1,$=0,C=0,A=0,M="";function _(e,r,t,n,a,s,i){return{value:e,root:r,parent:t,type:n,props:a,children:s,line:j,column:S,length:i,return:""}}function E(e,r){return h(_("",null,null,"",null,null,0),e,{length:-e.length},r)}function N(){return A=C>0?v(M,--C):0,S--,10===A&&(S=1,j--),A}function O(){return A=C<$?v(M,C++):0,S++,10===A&&(S=1,j++),A}function z(){return v(M,C)}function T(){return C}function P(e,r){return b(M,e,r)}function R(e){switch(e){case 0:case 9:case 10:case 13:case 32:return 5;case 33:case 43:case 44:case 47:case 62:case 64:case 126:case 59:case 123:case 125:return 4;case 58:return 3;case 34:case 39:case 40:case 91:return 2;case 41:case 93:return 1}return 0}function I(e){return j=S=1,$=y(M=e),C=0,[]}function B(e){return M="",e}function W(e){return x(P(C-1,G(91===e?e+2:40===e?e+1:e)))}function D(e){for(;(A=z())&&A<33;)O();return R(e)>2||R(A)>3?"":" "}function L(e,r){for(;--r&&O()&&!(A<48||A>102||A>57&&A<65||A>70&&A<97););return P(e,T()+(r<6&&32==z()&&32==O()))}function G(e){for(;O();)switch(A){case e:return C;case 34:case 39:34!==e&&39!==e&&G(A);break;case 40:41===e&&G(e);break;case 92:O()}return C}function H(e,r){for(;O()&&e+A!==57&&(e+A!==84||47!==z()););return"/*"+P(r,C-1)+"*"+f(47===e?e:O())}function U(e){for(;!R(z());)O();return P(e,C)}var V="-ms-",F="-moz-",Q="-webkit-",Y="comm",q="rule",K="decl",J="@keyframes";function X(e,r){for(var t="",n=w(e),a=0;a<n;a++)t+=r(e[a],a,e,r)||"";return t}function Z(e,r,t,n){switch(e.type){case"@layer":if(e.children.length)break;case"@import":case K:return e.return=e.return||e.value;case Y:return"";case J:return e.return=e.value+"{"+X(e.children,n)+"}";case q:e.value=e.props.join(",")}return y(t=X(e.children,n))?e.return=e.value+"{"+t+"}":""}function ee(e){return B(re("",null,null,null,[""],e=I(e),0,[0],e))}function re(e,r,t,n,a,s,i,o,c){for(var l=0,d=0,u=i,p=0,h=0,x=0,b=1,w=1,j=1,S=0,$="",C=a,A=s,M=n,_=$;w;)switch(x=S,S=O()){case 40:if(108!=x&&58==v(_,u-1)){-1!=m(_+=g(W(S),"&","&\f"),"&\f")&&(j=-1);break}case 34:case 39:case 91:_+=W(S);break;case 9:case 10:case 13:case 32:_+=D(x);break;case 92:_+=L(T()-1,7);continue;case 47:switch(z()){case 42:case 47:k(ne(H(O(),T()),r,t),c);break;default:_+="/"}break;case 123*b:o[l++]=y(_)*j;case 125*b:case 59:case 0:switch(S){case 0:case 125:w=0;case 59+d:-1==j&&(_=g(_,/\f/g,"")),h>0&&y(_)-u&&k(h>32?ae(_+";",n,t,u-1):ae(g(_," ","")+";",n,t,u-2),c);break;case 59:_+=";";default:if(k(M=te(_,r,t,l,d,a,o,$,C=[],A=[],u),s),123===S)if(0===d)re(_,r,M,M,C,s,u,o,A);else switch(99===p&&110===v(_,3)?100:p){case 100:case 108:case 109:case 115:re(e,M,M,n&&k(te(e,M,M,0,0,a,o,$,a,C=[],u),A),a,A,u,o,n?C:A);break;default:re(_,M,M,M,[""],A,0,o,A)}}l=d=h=0,b=j=1,$=_="",u=i;break;case 58:u=1+y(_),h=x;default:if(b<1)if(123==S)--b;else if(125==S&&0==b++&&125==N())continue;switch(_+=f(S),S*b){case 38:j=d>0?1:(_+="\f",-1);break;case 44:o[l++]=(y(_)-1)*j,j=1;break;case 64:45===z()&&(_+=W(O())),p=z(),d=u=y($=_+=U(T())),S++;break;case 45:45===x&&2==y(_)&&(b=0)}}return s}function te(e,r,t,n,a,s,i,o,c,l,d){for(var u=a-1,f=0===a?s:[""],h=w(f),m=0,v=0,y=0;m<n;++m)for(var k=0,j=b(e,u+1,u=p(v=i[m])),S=e;k<h;++k)(S=x(v>0?f[k]+" "+j:g(j,/&\f/g,f[k])))&&(c[y++]=S);return _(e,r,t,0===a?q:o,c,l,d)}function ne(e,r,t){return _(e,r,t,Y,f(A),b(e,2,-2),0)}function ae(e,r,t,n){return _(e,r,t,K,b(e,0,n),b(e,n+1,-1),n)}var se=function(e,r,t){for(var n=0,a=0;n=a,a=z(),38===n&&12===a&&(r[t]=1),!R(a);)O();return P(e,C)},ie=new WeakMap,oe=function(e){if("rule"===e.type&&e.parent&&!(e.length<1)){for(var r=e.value,t=e.parent,n=e.column===t.column&&e.line===t.line;"rule"!==t.type;)if(!(t=t.parent))return;if((1!==e.props.length||58===r.charCodeAt(0)||ie.get(t))&&!n){ie.set(e,!0);for(var a=[],s=function(e,r){return B(function(e,r){var t=-1,n=44;do{switch(R(n)){case 0:38===n&&12===z()&&(r[t]=1),e[t]+=se(C-1,r,t);break;case 2:e[t]+=W(n);break;case 4:if(44===n){e[++t]=58===z()?"&\f":"",r[t]=e[t].length;break}default:e[t]+=f(n)}}while(n=O());return e}(I(e),r))}(r,a),i=t.props,o=0,c=0;o<s.length;o++)for(var l=0;l<i.length;l++,c++)e.props[c]=a[o]?s[o].replace(/&\f/g,i[l]):i[l]+" "+s[o]}}},ce=function(e){if("decl"===e.type){var r=e.value;108===r.charCodeAt(0)&&98===r.charCodeAt(2)&&(e.return="",e.value="")}};function le(e,r){switch(function(e,r){return 45^v(e,0)?(((r<<2^v(e,0))<<2^v(e,1))<<2^v(e,2))<<2^v(e,3):0}(e,r)){case 5103:return Q+"print-"+e+e;case 5737:case 4201:case 3177:case 3433:case 1641:case 4457:case 2921:case 5572:case 6356:case 5844:case 3191:case 6645:case 3005:case 6391:case 5879:case 5623:case 6135:case 4599:case 4855:case 4215:case 6389:case 5109:case 5365:case 5621:case 3829:return Q+e+e;case 5349:case 4246:case 4810:case 6968:case 2756:return Q+e+F+e+V+e+e;case 6828:case 4268:return Q+e+V+e+e;case 6165:return Q+e+V+"flex-"+e+e;case 5187:return Q+e+g(e,/(\w+).+(:[^]+)/,Q+"box-$1$2"+V+"flex-$1$2")+e;case 5443:return Q+e+V+"flex-item-"+g(e,/flex-|-self/,"")+e;case 4675:return Q+e+V+"flex-line-pack"+g(e,/align-content|flex-|-self/,"")+e;case 5548:return Q+e+V+g(e,"shrink","negative")+e;case 5292:return Q+e+V+g(e,"basis","preferred-size")+e;case 6060:return Q+"box-"+g(e,"-grow","")+Q+e+V+g(e,"grow","positive")+e;case 4554:return Q+g(e,/([^-])(transform)/g,"$1"+Q+"$2")+e;case 6187:return g(g(g(e,/(zoom-|grab)/,Q+"$1"),/(image-set)/,Q+"$1"),e,"")+e;case 5495:case 3959:return g(e,/(image-set\([^]*)/,Q+"$1$`$1");case 4968:return g(g(e,/(.+:)(flex-)?(.*)/,Q+"box-pack:$3"+V+"flex-pack:$3"),/s.+-b[^;]+/,"justify")+Q+e+e;case 4095:case 3583:case 4068:case 2532:return g(e,/(.+)-inline(.+)/,Q+"$1$2")+e;case 8116:case 7059:case 5753:case 5535:case 5445:case 5701:case 4933:case 4677:case 5533:case 5789:case 5021:case 4765:if(y(e)-1-r>6)switch(v(e,r+1)){case 109:if(45!==v(e,r+4))break;case 102:return g(e,/(.+:)(.+)-([^]+)/,"$1"+Q+"$2-$3$1"+F+(108==v(e,r+3)?"$3":"$2-$3"))+e;case 115:return~m(e,"stretch")?le(g(e,"stretch","fill-available"),r)+e:e}break;case 4949:if(115!==v(e,r+1))break;case 6444:switch(v(e,y(e)-3-(~m(e,"!important")&&10))){case 107:return g(e,":",":"+Q)+e;case 101:return g(e,/(.+:)([^;!]+)(;|!.+)?/,"$1"+Q+(45===v(e,14)?"inline-":"")+"box$3$1"+Q+"$2$3$1"+V+"$2box$3")+e}break;case 5936:switch(v(e,r+11)){case 114:return Q+e+V+g(e,/[svh]\w+-[tblr]{2}/,"tb")+e;case 108:return Q+e+V+g(e,/[svh]\w+-[tblr]{2}/,"tb-rl")+e;case 45:return Q+e+V+g(e,/[svh]\w+-[tblr]{2}/,"lr")+e}return Q+e+V+e+e}return e}var de=[function(e,r,t,n){if(e.length>-1&&!e.return)switch(e.type){case K:e.return=le(e.value,e.length);break;case J:return X([E(e,{value:g(e.value,"@","@"+Q)})],n);case q:if(e.length)return function(e,r){return e.map(r).join("")}(e.props,function(r){switch(function(e){return(e=/(::plac\w+|:read-\w+)/.exec(e))?e[0]:e}(r)){case":read-only":case":read-write":return X([E(e,{props:[g(r,/:(read-\w+)/,":-moz-$1")]})],n);case"::placeholder":return X([E(e,{props:[g(r,/:(plac\w+)/,":"+Q+"input-$1")]}),E(e,{props:[g(r,/:(plac\w+)/,":-moz-$1")]}),E(e,{props:[g(r,/:(plac\w+)/,V+"input-$1")]})],n)}return""})}}],ue={animationIterationCount:1,aspectRatio:1,borderImageOutset:1,borderImageSlice:1,borderImageWidth:1,boxFlex:1,boxFlexGroup:1,boxOrdinalGroup:1,columnCount:1,columns:1,flex:1,flexGrow:1,flexPositive:1,flexShrink:1,flexNegative:1,flexOrder:1,gridRow:1,gridRowEnd:1,gridRowSpan:1,gridRowStart:1,gridColumn:1,gridColumnEnd:1,gridColumnSpan:1,gridColumnStart:1,msGridRow:1,msGridRowSpan:1,msGridColumn:1,msGridColumnSpan:1,fontWeight:1,lineHeight:1,opacity:1,order:1,orphans:1,scale:1,tabSize:1,widows:1,zIndex:1,zoom:1,WebkitLineClamp:1,fillOpacity:1,floodOpacity:1,stopOpacity:1,strokeDasharray:1,strokeDashoffset:1,strokeMiterlimit:1,strokeOpacity:1,strokeWidth:1};function pe(e){var r=Object.create(null);return function(t){return void 0===r[t]&&(r[t]=e(t)),r[t]}}var fe=/[A-Z]|^ms/g,he=/_EMO_([^_]+?)_([^]*?)_EMO_/g,xe=function(e){return 45===e.charCodeAt(1)},ge=function(e){return null!=e&&"boolean"!=typeof e},me=pe(function(e){return xe(e)?e:e.replace(fe,"-$&").toLowerCase()}),ve=function(e,r){switch(e){case"animation":case"animationName":if("string"==typeof r)return r.replace(he,function(e,r,t){return ye={name:r,styles:t,next:ye},r})}return 1===ue[e]||xe(e)||"number"!=typeof r||0===r?r:r+"px"};function be(e,r,t){if(null==t)return"";var n=t;if(void 0!==n.__emotion_styles)return n;switch(typeof t){case"boolean":return"";case"object":var a=t;if(1===a.anim)return ye={name:a.name,styles:a.styles,next:ye},a.name;var s=t;if(void 0!==s.styles){var i=s.next;if(void 0!==i)for(;void 0!==i;)ye={name:i.name,styles:i.styles,next:ye},i=i.next;return s.styles+";"}return function(e,r,t){var n="";if(Array.isArray(t))for(var a=0;a<t.length;a++)n+=be(e,r,t[a])+";";else for(var s in t){var i=t[s];if("object"!=typeof i){var o=i;null!=r&&void 0!==r[o]?n+=s+"{"+r[o]+"}":ge(o)&&(n+=me(s)+":"+ve(s,o)+";")}else if(!Array.isArray(i)||"string"!=typeof i[0]||null!=r&&void 0!==r[i[0]]){var c=be(e,r,i);switch(s){case"animation":case"animationName":n+=me(s)+":"+c+";";break;default:n+=s+"{"+c+"}"}}else for(var l=0;l<i.length;l++)ge(i[l])&&(n+=me(s)+":"+ve(s,i[l])+";")}return n}(e,r,t);case"function":if(void 0!==e){var o=ye,c=t(e);return ye=o,be(e,r,c)}}var l=t;if(null==r)return l;var d=r[l];return void 0!==d?d:l}var ye,we=/label:\s*([^\s;{]+)\s*(;|$)/g;function ke(e,r,t){if(1===e.length&&"object"==typeof e[0]&&null!==e[0]&&void 0!==e[0].styles)return e[0];var n=!0,a="";ye=void 0;var s=e[0];null==s||void 0===s.raw?(n=!1,a+=be(t,r,s)):a+=s[0];for(var i=1;i<e.length;i++)a+=be(t,r,e[i]),n&&(a+=s[i]);we.lastIndex=0;for(var o,c="";null!==(o=we.exec(a));)c+="-"+o[1];var l=function(e){for(var r,t=0,n=0,a=e.length;a>=4;++n,a-=4)r=1540483477*(65535&(r=255&e.charCodeAt(n)|(255&e.charCodeAt(++n))<<8|(255&e.charCodeAt(++n))<<16|(255&e.charCodeAt(++n))<<24))+(59797*(r>>>16)<<16),t=1540483477*(65535&(r^=r>>>24))+(59797*(r>>>16)<<16)^1540483477*(65535&t)+(59797*(t>>>16)<<16);switch(a){case 3:t^=(255&e.charCodeAt(n+2))<<16;case 2:t^=(255&e.charCodeAt(n+1))<<8;case 1:t=1540483477*(65535&(t^=255&e.charCodeAt(n)))+(59797*(t>>>16)<<16)}return(((t=1540483477*(65535&(t^=t>>>13))+(59797*(t>>>16)<<16))^t>>>15)>>>0).toString(36)}(a)+c;return{name:l,styles:a,next:ye}}function je(e,r,t){var n="";return t.split(" ").forEach(function(t){void 0!==e[t]?r.push(e[t]+";"):t&&(n+=t+" ")}),n}function Se(e,r){if(void 0===e.inserted[r.name])return e.insert("",r,e.sheet,!0)}function $e(e,r,t){var n=[],a=je(e,n,t);return n.length<2?t:a+r(n)}var Ce=function e(r){for(var t="",n=0;n<r.length;n++){var a=r[n];if(null!=a){var s=void 0;switch(typeof a){case"boolean":break;case"object":if(Array.isArray(a))s=e(a);else for(var i in s="",a)a[i]&&i&&(s&&(s+=" "),s+=i);break;default:s=a}s&&(t&&(t+=" "),t+=s)}}return t},Ae=function(){var e=function(e){var r=e.key;if("css"===r){var t=document.querySelectorAll("style[data-emotion]:not([data-s])");Array.prototype.forEach.call(t,function(e){-1!==e.getAttribute("data-emotion").indexOf(" ")&&(document.head.appendChild(e),e.setAttribute("data-s",""))})}var n,a,s=e.stylisPlugins||de,i={},o=[];n=e.container||document.head,Array.prototype.forEach.call(document.querySelectorAll('style[data-emotion^="'+r+' "]'),function(e){for(var r=e.getAttribute("data-emotion").split(" "),t=1;t<r.length;t++)i[r[t]]=!0;o.push(e)});var c,l,d,p,f=[Z,(p=function(e){c.insert(e)},function(e){e.root||(e=e.return)&&p(e)})],h=(l=[oe,ce].concat(s,f),d=w(l),function(e,r,t,n){for(var a="",s=0;s<d;s++)a+=l[s](e,r,t,n)||"";return a});a=function(e,r,t,n){c=t,X(ee(e?e+"{"+r.styles+"}":r.styles),h),n&&(x.inserted[r.name]=!0)};var x={key:r,sheet:new u({key:r,container:n,nonce:e.nonce,speedy:e.speedy,prepend:e.prepend,insertionPoint:e.insertionPoint}),nonce:e.nonce,inserted:i,registered:{},insert:a};return x.sheet.hydrate(o),x}({key:"css"});e.sheet.speedy=function(e){this.isSpeedy=e},e.compat=!0;var r=function(){for(var r=arguments.length,t=new Array(r),n=0;n<r;n++)t[n]=arguments[n];var a=ke(t,e.registered,void 0);return function(e,r){!function(e,r){var t=e.key+"-"+r.name;void 0===e.registered[t]&&(e.registered[t]=r.styles)}(e,r);var t=e.key+"-"+r.name;if(void 0===e.inserted[r.name]){var n=r;do{e.insert(r===n?"."+t:"",n,e.sheet,!0),n=n.next}while(void 0!==n)}}(e,a),e.key+"-"+a.name};return{css:r,cx:function(){for(var t=arguments.length,n=new Array(t),a=0;a<t;a++)n[a]=arguments[a];return $e(e.registered,r,Ce(n))},injectGlobal:function(){for(var r=arguments.length,t=new Array(r),n=0;n<r;n++)t[n]=arguments[n];var a=ke(t,e.registered);Se(e,a)},keyframes:function(){for(var r=arguments.length,t=new Array(r),n=0;n<r;n++)t[n]=arguments[n];var a=ke(t,e.registered),s="animation-"+a.name;return Se(e,{name:a.name,styles:"@keyframes "+s+"{"+a.styles+"}"}),s},hydrate:function(r){r.forEach(function(r){e.inserted[r]=!0})},flush:function(){e.registered={},e.inserted={},e.sheet.flush()},sheet:e.sheet,cache:e,getRegisteredStyles:je.bind(null,e.registered),merge:$e.bind(null,e.registered,r)}}(),Me=(Ae.flush,Ae.hydrate,Ae.cx),_e=(Ae.merge,Ae.getRegisteredStyles,Ae.injectGlobal,Ae.keyframes,Ae.css);function Ee(e){const r=[];return e.split(/(```[\s\S]*?```)/g).forEach((e,t)=>{if(e.startsWith("```")){const n=e.replace(/^```[^\n]*\n?/,"").replace(/```$/,"");return void r.push((0,c.jsx)("pre",{style:{background:"var(--color-bg-secondary,#1a1a2e)",padding:"8px 12px",borderRadius:4,overflowX:"auto",fontSize:11,margin:"6px 0",lineHeight:1.5},children:(0,c.jsx)("code",{children:n})},t))}const n=e.split("\n");let a=!1;n.forEach((e,n)=>{const s=/^[\-\*]\s+(.+)/.exec(e);if(s)return a||(a=!0),void r.push((0,c.jsx)("li",{style:{marginLeft:16,listStyleType:"disc"},children:Ne(s[1])},`${t}-${n}`));a&&(a=!1),""!==e?r.push((0,c.jsx)("span",{style:{display:"block",marginBottom:2},children:Ne(e)},`${t}-${n}`)):r.push((0,c.jsx)("br",{},`${t}-${n}`))})}),r}function Ne(e){const r=e.split(/(`[^`]+`)/g);return(0,c.jsx)(c.Fragment,{children:r.map((e,r)=>e.startsWith("`")&&e.endsWith("`")?(0,c.jsx)("code",{style:{background:"rgba(255,255,255,0.1)",padding:"1px 4px",borderRadius:3,fontSize:"0.9em"},children:e.slice(1,-1)},r):(0,c.jsx)("span",{dangerouslySetInnerHTML:{__html:e.replace(/\*\*(.+?)\*\*/g,"<strong>$1</strong>").replace(/\*(.+?)\*/g,"<em>$1</em>")}},r))})}Ae.sheet,Ae.cache;const Oe=()=>(0,c.jsxs)("span",{style:{display:"inline-flex",gap:3,alignItems:"center",height:16},children:[[0,1,2].map(e=>(0,c.jsx)("span",{style:{width:6,height:6,borderRadius:"50%",background:"var(--text-secondary,#aaa)",animation:`bounce 1.2s ${.2*e}s infinite`}},e)),(0,c.jsx)("style",{children:"@keyframes bounce { 0%,80%,100%{transform:translateY(0)} 40%{transform:translateY(-5px)} }"})]}),ze=new o.PanelPlugin(({options:e,width:r,height:t,timeRange:n,replaceVariables:a})=>{const[s,i]=(0,l.useState)(""),[o,u]=(0,l.useState)([]),[p,f]=(0,l.useState)(!1),h=e.backendUrl||"http://localhost:8008",x=e.systemPrompt||"",g=e.title||"DAFS AI Assistant",m=(0,l.useRef)(null),v=(0,l.useRef)(null);(0,l.useEffect)(()=>{m.current&&(m.current.scrollTop=m.current.scrollHeight)},[o,p]);const b=(0,l.useCallback)(e=>e.map(e=>({role:e.role,content:e.content})),[]),y=(0,l.useCallback)(async e=>{const r=e.trim();if(!r||p)return;const t={role:"user",content:r,ts:Date.now()};u(e=>[...e,t]),f(!0),i("");const s=b(o);try{const e=await fetch(`${h}/query`,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({query:a(r),context:{dashboardTitle:document.title,from:n.from.toISOString(),to:n.to.toISOString()},systemPrompt:x,history:s})});if(!e.ok){const r=await e.text().catch(()=>"");throw new Error(`HTTP ${e.status}${r?": "+r.slice(0,200):""}`)}const t=await e.json();u(e=>[...e,{role:"assistant",content:t.answer,ts:Date.now(),esQueries:t.tool_calls_made}])}catch(e){const r=e instanceof Error?e.message:String(e);u(e=>[...e,{role:"assistant",content:`⚠️ Error: ${r}`,ts:Date.now()}])}finally{f(!1),setTimeout(()=>v.current?.focus(),50)}},[p,o,h,x,n,a,b]),w=(0,l.useMemo)(()=>(({width:e,height:r})=>({container:_e`
    width: ${e}px;
    height: ${r}px;
    display: flex;
    flex-direction: column;
    font-size: 13px;
    background: var(--background-primary, #111);
    overflow: hidden;
  `,header:_e`
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 8px 12px;
    border-bottom: 1px solid var(--border-weak, #333);
    background: var(--background-secondary, #1a1a1a);
    flex-shrink: 0;
  `,headerTitle:_e`
    font-weight: 600;
    font-size: 13px;
    display: flex;
    align-items: center;
    color: var(--text-primary, #d8d9da);
    letter-spacing: 0.01em;
  `,clearBtn:_e`
    display: flex;
    align-items: center;
    gap: 4px;
    background: none;
    border: 1px solid var(--border-weak, #444);
    border-radius: 4px;
    color: var(--text-secondary, #999);
    font-size: 11px;
    padding: 3px 8px;
    cursor: pointer;
    &:hover {
      background: var(--background-secondary, #252525);
      color: var(--text-primary, #ddd);
      border-color: var(--border-medium, #666);
    }
  `,chat:_e`
    flex: 1;
    overflow-y: auto;
    padding: 12px 12px 4px;
    display: flex;
    flex-direction: column;
    gap: 12px;
    scroll-behavior: smooth;
    &::-webkit-scrollbar { width: 4px; }
    &::-webkit-scrollbar-track { background: transparent; }
    &::-webkit-scrollbar-thumb { background: var(--border-medium, #444); border-radius: 4px; }
  `,empty:_e`
    flex: 1;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    color: var(--text-secondary, #888);
    font-size: 12px;
    text-align: center;
    gap: 2px;
    padding: 24px;
  `,row:_e`
    display: flex;
    align-items: flex-end;
    gap: 8px;
    animation: fadeIn 0.15s ease-in;
    @keyframes fadeIn { from { opacity: 0; transform: translateY(4px); } to { opacity: 1; transform: translateY(0); } }
  `,rowUser:_e`
    flex-direction: row-reverse;
  `,rowAssistant:_e`
    flex-direction: row;
  `,avatar:_e`
    width: 24px;
    height: 24px;
    border-radius: 50%;
    background: var(--primary-shade, #1e6091);
    display: flex;
    align-items: center;
    justify-content: center;
    flex-shrink: 0;
    color: #fff;
  `,bubbleUser:_e`
    max-width: 80%;
    background: var(--primary-main, #3d71ab);
    color: #fff;
    border-radius: 14px 14px 4px 14px;
    padding: 8px 12px;
    line-height: 1.5;
    box-shadow: 0 1px 3px rgba(0,0,0,0.2);
  `,bubbleAssistant:_e`
    max-width: 85%;
    background: var(--background-secondary, #1f2128);
    border: 1px solid var(--border-weak, #333);
    border-radius: 14px 14px 14px 4px;
    padding: 8px 12px;
    line-height: 1.5;
    box-shadow: 0 1px 3px rgba(0,0,0,0.15);
    color: var(--text-primary, #d8d9da);
  `,bubbleContent:_e`
    font-size: 13px;
    word-break: break-word;
  `,bubbleMeta:_e`
    display: flex;
    align-items: center;
    gap: 8px;
    margin-top: 6px;
    justify-content: flex-end;
  `,esBadge:_e`
    display: inline-flex;
    align-items: center;
    font-size: 10px;
    color: var(--text-secondary, #aaa);
    background: rgba(255,255,255,0.06);
    border: 1px solid var(--border-weak, #444);
    border-radius: 10px;
    padding: 1px 6px;
  `,timestamp:_e`
    font-size: 10px;
    color: var(--text-disabled, #666);
    flex-shrink: 0;
  `,composer:_e`
    padding: 8px 12px 10px;
    border-top: 1px solid var(--border-weak, #333);
    background: var(--background-secondary, #1a1a1a);
    flex-shrink: 0;
  `,inputWrapper:_e`
    display: flex;
    gap: 8px;
    align-items: flex-end;
  `,textarea:_e`
    flex: 1;
    resize: none;
    background: var(--background-primary, #111);
    border: 1px solid var(--border-medium, #444);
    border-radius: 8px;
    color: var(--text-primary, #d8d9da);
    font-size: 13px;
    padding: 8px 10px;
    line-height: 1.5;
    outline: none;
    font-family: inherit;
    &:focus {
      border-color: var(--primary-shade, #3d71ab);
      box-shadow: 0 0 0 2px rgba(61,113,171,0.2);
    }
    &::placeholder {
      color: var(--text-disabled, #555);
    }
    &:disabled {
      opacity: 0.5;
      cursor: not-allowed;
    }
  `,sendBtn:_e`
    flex-shrink: 0;
    height: 36px;
    width: 36px;
    padding: 0 !important;
    display: flex;
    align-items: center;
    justify-content: center;
    border-radius: 8px !important;
  `}))({width:r,height:t}),[r,t]);return(0,c.jsxs)("div",{className:w.container,children:[(0,c.jsxs)("div",{className:w.header,children:[(0,c.jsxs)("span",{className:w.headerTitle,children:[(0,c.jsxs)("svg",{width:"14",height:"14",viewBox:"0 0 24 24",fill:"none",stroke:"currentColor",strokeWidth:"2",style:{marginRight:6,flexShrink:0},children:[(0,c.jsx)("circle",{cx:"12",cy:"12",r:"3"}),(0,c.jsx)("path",{d:"M12 1v4M12 19v4M4.22 4.22l2.83 2.83M16.95 16.95l2.83 2.83M1 12h4M19 12h4M4.22 19.78l2.83-2.83M16.95 7.05l2.83-2.83"})]}),g]}),o.length>0&&(0,c.jsxs)("button",{className:w.clearBtn,onClick:()=>u([]),title:"Limpiar conversación",children:[(0,c.jsxs)("svg",{width:"12",height:"12",viewBox:"0 0 24 24",fill:"none",stroke:"currentColor",strokeWidth:"2",children:[(0,c.jsx)("polyline",{points:"3 6 5 6 21 6"}),(0,c.jsx)("path",{d:"M19 6l-1 14a2 2 0 0 1-2 2H8a2 2 0 0 1-2-2L5 6"}),(0,c.jsx)("path",{d:"M10 11v6M14 11v6"}),(0,c.jsx)("path",{d:"M9 6V4a1 1 0 0 1 1-1h4a1 1 0 0 1 1 1v2"})]}),"Limpiar"]})]}),(0,c.jsxs)("div",{className:w.chat,ref:m,children:[0===o.length&&!p&&(0,c.jsxs)("div",{className:w.empty,children:[(0,c.jsx)("svg",{width:"32",height:"32",viewBox:"0 0 24 24",fill:"none",stroke:"currentColor",strokeWidth:"1.5",style:{opacity:.3,marginBottom:8},children:(0,c.jsx)("path",{d:"M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"})}),(0,c.jsx)("span",{children:"Haz una pregunta sobre el dashboard o los datos de auditoría."}),(0,c.jsx)("span",{style:{fontSize:11,marginTop:4,opacity:.5},children:"Enter para enviar · Shift+Enter para salto de línea"})]}),o.map((e,r)=>(0,c.jsxs)("div",{className:Me(w.row,"user"===e.role?w.rowUser:w.rowAssistant),children:["assistant"===e.role&&(0,c.jsx)("div",{className:w.avatar,children:(0,c.jsxs)("svg",{width:"12",height:"12",viewBox:"0 0 24 24",fill:"currentColor",children:[(0,c.jsx)("circle",{cx:"12",cy:"12",r:"3"}),(0,c.jsx)("path",{d:"M12 1v4M12 19v4M4.22 4.22l2.83 2.83M16.95 16.95l2.83 2.83M1 12h4M19 12h4M4.22 19.78l2.83-2.83M16.95 7.05l2.83-2.83"})]})}),(0,c.jsxs)("div",{className:"user"===e.role?w.bubbleUser:w.bubbleAssistant,children:[(0,c.jsx)("div",{className:w.bubbleContent,children:Ee(e.content)}),(0,c.jsxs)("div",{className:w.bubbleMeta,children:["number"==typeof e.esQueries&&e.esQueries>0&&(0,c.jsxs)("span",{className:w.esBadge,title:"Consultas Elasticsearch realizadas",children:[(0,c.jsx)("svg",{width:"9",height:"9",viewBox:"0 0 24 24",fill:"currentColor",style:{marginRight:3},children:(0,c.jsx)("path",{d:"M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-1 15H9V8h2v9zm4 0h-2V8h2v9z"})}),e.esQueries," ",1===e.esQueries?"consulta ES":"consultas ES"]}),(0,c.jsx)("span",{className:w.timestamp,children:new Date(e.ts).toLocaleTimeString([],{hour:"2-digit",minute:"2-digit"})})]})]})]},`${e.ts}-${r}`)),p&&(0,c.jsxs)("div",{className:Me(w.row,w.rowAssistant),children:[(0,c.jsx)("div",{className:w.avatar,children:(0,c.jsxs)("svg",{width:"12",height:"12",viewBox:"0 0 24 24",fill:"currentColor",children:[(0,c.jsx)("circle",{cx:"12",cy:"12",r:"3"}),(0,c.jsx)("path",{d:"M12 1v4M12 19v4M4.22 4.22l2.83 2.83M16.95 16.95l2.83 2.83M1 12h4M19 12h4M4.22 19.78l2.83-2.83M16.95 7.05l2.83-2.83"})]})}),(0,c.jsx)("div",{className:w.bubbleAssistant,style:{padding:"10px 14px"},children:(0,c.jsx)(Oe,{})})]})]}),(0,c.jsx)("div",{className:w.composer,children:(0,c.jsxs)("div",{className:w.inputWrapper,children:[(0,c.jsx)("textarea",{ref:v,className:w.textarea,rows:2,value:s,onChange:e=>i(e.target.value),onKeyDown:e=>{"Enter"!==e.key||e.shiftKey||(e.preventDefault(),y(s))},placeholder:"Escribe tu pregunta… (Enter para enviar)",disabled:p}),(0,c.jsx)(d.Button,{className:w.sendBtn,disabled:p||!s.trim(),onClick:()=>y(s),size:"sm",variant:"primary",children:p?(0,c.jsx)(d.Spinner,{inline:!0,size:"sm"}):(0,c.jsxs)("svg",{width:"14",height:"14",viewBox:"0 0 24 24",fill:"none",stroke:"currentColor",strokeWidth:"2.5",children:[(0,c.jsx)("line",{x1:"22",y1:"2",x2:"11",y2:"13"}),(0,c.jsx)("polygon",{points:"22 2 15 22 11 13 2 9 22 2"})]})})]})})]})}).setPanelOptions(e=>e.addTextInput({path:"title",name:"Panel title",defaultValue:"DAFS AI Assistant"}).addTextInput({path:"backendUrl",name:"Backend URL",description:"Ejemplo: http://localhost:8008",defaultValue:"http://localhost:8008"}).addTextInput({path:"systemPrompt",name:"System prompt",defaultValue:"Eres un asistente de observabilidad para dashboards de Grafana."}));return i})());