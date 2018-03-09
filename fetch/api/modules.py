import hashlib, hmac, base64

# generate X-Signature
def gen_signature(time_now, http_method, request_uri, x_secrete):
	message = time_now + "." + http_method + "." + request_uri
	signature = hmac.new(bytes(x_secrete, "utf-8"), message.encode("utf-8"), hashlib.sha256).digest()
	b64_enc_signature = str(base64.b64encode(signature), "utf-8")
	return b64_enc_signature
