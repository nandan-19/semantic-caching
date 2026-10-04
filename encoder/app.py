import time
import grpc
from concurrent import futures
from sentence_transformers import SentenceTransformer
import laya

import api_pb2
import api_pb2_grpc

print("Loading SentenceTransformer model...")
model = SentenceTransformer("all-MiniLM-L6-v2")
print("Model loaded successfully.")
print("Loading Laya verifier model...")
try:
    laya_model = laya.load("convaiinnovations/laya")
    print("Laya verifier model loaded successfully.")
except Exception as error:
    laya_model = None
    print(f"Laya verifier unavailable; semantic candidates will miss: {error}")


class EncoderService(api_pb2_grpc.EncoderServiceServicer):
    def GetEmbedding(self, request, context):
        """
        Receives a TextRequest, encodes the text, and returns an EmbeddingResponse.
        """
        start_time = time.time()

        raw_text = request.text

        vector = model.encode(raw_text).tolist()

        elapsed_ms = (time.time() - start_time) * 1000
        print(f"Encoded query in {elapsed_ms:.2f}ms: '{raw_text}'")

        return api_pb2.EmbeddingResponse(vector=vector)

    def VerifyCachedResponse(self, request, context):
        """Return P(true) for whether a semantic-cache candidate is safe to reuse."""
        try:
            if laya_model is None:
                context.abort(
                    grpc.StatusCode.UNAVAILABLE, "Laya verifier is unavailable"
                )
            result = laya_model.predict(
                {
                    "new_query": request.new_query,
                    "cached_query": request.cached_query,
                    "cached_response": request.cached_response,
                },
                {
                    "safe_to_return": {
                        "type": "noul",
                        "instructions": "Can the cached response safely be returned for the new query?",
                    }
                },
            )
            probability = result["answers"]["safe_to_return"]["noul"]
            return api_pb2.CacheVerificationResponse(probability=probability)
        except Exception as error:
            context.abort(
                grpc.StatusCode.UNAVAILABLE, f"Laya verification failed: {error}"
            )


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    api_pb2_grpc.add_EncoderServiceServicer_to_server(EncoderService(), server)

    server.add_insecure_port("[::]:50052")
    server.start()
    print("Encoder Service running on port 50052...")

    server.wait_for_termination()


if __name__ == "__main__":
    serve()
