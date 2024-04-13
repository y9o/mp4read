// Package mp4read provides for reading MP4
package mp4read

import (
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"time"

	"github.com/abema/go-mp4"
	"github.com/sunfish-shogi/bufseekio"
)

type mp4videoRead struct {
	chunkIdx     int
	sampleIdx    int
	sampleEnd    int
	decodingTime int64
	dataOffset   int64
}
type Mp4read struct {
	mp4fh      *os.File
	r          io.ReadSeeker
	probe      *mp4.ProbeInfo
	track      *mp4.Track
	read       mp4videoRead
	spspps     [][]byte
	stss       []uint32 //IDR
	_starttime int      //１フレーム目のCompositionTimeを0に補正するための値
}

// VideoSampleInfo
//
// ビデオのサンプル情報。ReadMdatAtSampleに渡すとサンプルデータを取得できる。
type VideoSampleInfo struct {
	offset          int64
	size            uint32
	NalLengthSize   int    // サンプルデータ先頭にある長さ情報のサイズ(byte)
	TimeDelta       uint32 // サンプルの再生時間
	Number          int64  // サンプルの番号。フレーム番号ではない。
	CompositionTime int64  // 1サンプル目を0とした時の再生時刻
}

// VideoInfo
//
// ビデオトラックの情報。
type VideoInfo struct {
	Width     int
	Height    int
	Duration  int64 // Duration()
	Timescale int64 // Timescale()
	Samples   int   // フレーム数
}

var ErrEndOfStream = fmt.Errorf("end of stream")

// mp4 headerの情報をもとにメモリ確保をします。
var MemoryLimitSampleCapacity int = 4 * 1024 * 1024

// NewFromFile
//
// mp4ファイルを読み込みます。
func NewFromFile(mp4filename string) (*Mp4read, error) {
	fh, err := os.Open(mp4filename)
	if err != nil {
		return nil, err
	}

	r := bufseekio.NewReadSeeker(fh, 1024, 4)
	ret, err := NewFromReadSeeker(r)
	if err != nil {
		fh.Close()
		return nil, err
	}
	ret.mp4fh = fh

	return ret, nil
}

// NewFromReadSeeker
//
// mp4を読み込みます。
func NewFromReadSeeker(r io.ReadSeeker) (*Mp4read, error) {
	info, err := mp4.Probe(r)
	if err != nil {
		return nil, err
	}
	ret := &Mp4read{
		r:     r,
		probe: info,
	}
	if err := ret.SetVideoTrack(-1); err != nil {
		ret.Close()
		return nil, err
	}
	return ret, nil
}

// Close
//
// 必要な後処理を行います。
func (v *Mp4read) Close() error {
	if v.mp4fh != nil {
		err := v.mp4fh.Close()
		v.mp4fh = nil
		if err != nil {
			return err
		}
	}
	return nil
}

// Duration
//
// Videoの長さを返します。1sec==Timescale()
func (v *Mp4read) Duration() int64 {
	if v.track == nil {
		return 0
	}
	return int64(v.track.Duration)
}

// TimeDuration
//
// time.DurationでVideoの長さを返します。
func (v *Mp4read) TimeDuration() time.Duration {
	if v.track == nil || v.track.Timescale == 0 {
		return 0
	}
	return time.Duration(uint64(v.track.Duration) * uint64(time.Second) / uint64(v.track.Timescale))
}

// Timescale
//
// Videoの1秒の単位を返します。
func (v *Mp4read) Timescale() uint32 {
	if v.track == nil {
		return 0
	}
	return v.track.Timescale
}

// VideoInfo
//
// 選択されているビデオトラックの情報
func (v *Mp4read) VideoInfo() (*VideoInfo, error) {
	if v.track == nil {
		return nil, errors.New("video not found")
	}
	return &VideoInfo{
		Width:     int(v.track.AVC.Width),
		Height:    int(v.track.AVC.Height),
		Duration:  int64(v.track.Duration),
		Timescale: int64(v.track.Timescale),
		Samples:   len(v.track.Samples),
	}, nil
}

// GetVideoTracks
//
// mp4に含まれるビデオトラックのリスト
func (v *Mp4read) GetVideoTracks() (r []int64) {
	for _, track := range v.probe.Tracks {
		if track.AVC == nil {
			continue
		}
		r = append(r, int64(track.TrackID))
	}
	return
}

// SetVideoTrack
//
// Initialize()の前に取り出す対象のビデオトラックを指定。-1なら最初のビデオトラック。
func (v *Mp4read) SetVideoTrack(id int64) error {
	v.track = nil
	v.stss = nil
	v.spspps = nil
	for _, track := range v.probe.Tracks {
		if track.AVC == nil {
			continue
		}
		if id != -1 && id != int64(track.TrackID) {
			continue
		}

		v.track = track
		return nil
	}
	return fmt.Errorf("video track not found: #%d", id)
}

// Initialize
//
// video trackの詳細を読み込みます。
func (v *Mp4read) Initialize() error {

	if v.track == nil {
		return fmt.Errorf("video track not found")
	}

	if err := v.loadTrackInfo(v.track.TrackID); err != nil {
		return err
	}

	//bフレームの影響でcttsのCompositionTimeOffsetで1フレーム目の時間が0にならない時の補正
	starttime := 2147483647
	decodingTime := 0
	for _, sample := range v.track.Samples {
		starttime = min(starttime, decodingTime+int(sample.CompositionTimeOffset))
		if sample.CompositionTimeOffset == 0 {
			break
		}
		decodingTime += int(sample.TimeDelta)
	}
	if starttime != 2147483647 {
		v._starttime = starttime
	}
	return nil
}

// GetSPSPPS
//
// avcC Box内のSPS/PPSデータ
func (v *Mp4read) GetSPSPPS() [][]byte {
	return v.spspps
}

// loadTrackInfo h264デコードに必要な指定IDのSPSPPS情報などを読み取る
func (v *Mp4read) loadTrackInfo(trackid uint32) error {
	bips, err := mp4.ExtractBoxesWithPayload(v.r, nil, []mp4.BoxPath{
		{mp4.BoxTypeMoov(), mp4.BoxTypeTrak(), mp4.BoxTypeTkhd()},
		{mp4.BoxTypeMoov(), mp4.BoxTypeTrak(), mp4.BoxTypeMdia(), mp4.BoxTypeMinf(), mp4.BoxTypeStbl(), mp4.BoxTypeStss()},
		{mp4.BoxTypeMoov(), mp4.BoxTypeTrak(), mp4.BoxTypeMdia(), mp4.BoxTypeMinf(), mp4.BoxTypeStbl(), mp4.BoxTypeStsd(), mp4.BoxTypeAvc1(), mp4.BoxTypeAvcC()},
	})
	if err != nil {
		return err
	}
	target := false
	for _, bip := range bips {
		switch bip.Info.Type {
		case mp4.BoxTypeTkhd():
			tkhd := bip.Payload.(*mp4.Tkhd)
			target = tkhd.TrackID == trackid
		case mp4.BoxTypeStss():
			if !target {
				continue
			}
			// Seek時に使うIframeの一覧
			stss := bip.Payload.(*mp4.Stss)
			v.stss = slices.Clone(stss.SampleNumber)

		case mp4.BoxTypeAvcC():
			if !target {
				continue
			}
			// mdatにSPSPPSが無い時に必要
			v.spspps = make([][]byte, 0, 4)
			avcC := bip.Payload.(*mp4.AVCDecoderConfiguration)
			for _, sps := range avcC.SequenceParameterSets {
				v.spspps = append(v.spspps, sps.NALUnit)
			}
			for _, pps := range avcC.PictureParameterSets {
				v.spspps = append(v.spspps, pps.NALUnit)
			}
		}
	}
	if v.spspps == nil {
		return errors.New("avcC not found")
	}
	return nil
}

// NextSample
//
// 次のSampleデータを計算してinfoに代入します。終わりに到達するとErrEndOfStreamを返します。
func (v *Mp4read) NextSample(out *VideoSampleInfo) error {
	if v.track == nil {
		return fmt.Errorf("video track not found")
	}
	out.NalLengthSize = int(v.track.AVC.LengthSize)
	for v.read.chunkIdx < len(v.track.Chunks) {
		chunk := v.track.Chunks[v.read.chunkIdx]
		if v.read.sampleEnd == 0 {
			v.read.sampleEnd = v.read.sampleIdx + int(chunk.SamplesPerChunk)
			v.read.dataOffset = int64(chunk.DataOffset)
		}
		for v.read.sampleIdx < v.read.sampleEnd && v.read.sampleIdx < len(v.track.Samples) {
			sample := v.track.Samples[v.read.sampleIdx]

			out.offset = v.read.dataOffset
			out.size = sample.Size
			out.CompositionTime = v.read.decodingTime + sample.CompositionTimeOffset - int64(v._starttime)
			out.Number = int64(v.read.sampleIdx)
			out.TimeDelta = sample.TimeDelta

			v.read.sampleIdx++
			v.read.dataOffset += int64(sample.Size)
			v.read.decodingTime += int64(sample.TimeDelta)
			if sample.Size > 0 {
				return nil
			}
		}
		v.read.sampleEnd = 0
		v.read.chunkIdx++
	}
	return ErrEndOfStream
}

// ReadMdatAtSample
//
// mdatからSampleデータを読み込みます。
func (v *Mp4read) ReadMdatAtSample(info *VideoSampleInfo, buf []byte) (avc []byte, err error) {
	if info.size > uint32(MemoryLimitSampleCapacity) {
		return buf, fmt.Errorf("sample size capacity over: %d >= MemoryLimitSampleCapacity(%d)", info.size, MemoryLimitSampleCapacity)
	}
	if _, err := v.r.Seek(info.offset, io.SeekStart); err != nil {
		return buf, err
	}
	if info.size > uint32(cap(buf)) {
		avc = make([]byte, info.size)
	} else {
		avc = buf[:info.size]
	}
	var n int
	if n, err = io.ReadFull(v.r, avc); err != nil {
		return
	} else if n != int(info.size) {
		err = fmt.Errorf("cant read %d bytes. len()==%d", info.size, n)
	}
	return
}

// Seek
//
// Timescale()単位の指定時刻より前のIDRに移動
//
//	force: true 現在より未来のtimestampでも、現在より過去のIDRに移動。
//	       false 同GOP内で、未来のtimestampなら移動しない。該当したら戻り値はfalse
func (v *Mp4read) Seek(timestamp int64, force bool) (bool, error) {
	if v.track == nil {
		return false, errors.New("track not found")
	}
	if v.stss == nil {
		//stssが無ければ作成する。時間がかかる。
		stss, err := mp4.FindIDRFrames(v.r, v.track)
		if err != nil {
			return false, err
		}
		v.stss = make([]uint32, len(stss))
		for i := range stss {
			v.stss[i] = uint32(stss[i]) + 1
		}
	}
	var sampleIndex int
	var stssIdx int
	var decodingTime int64
	var idr mp4videoRead

	timestamp += int64(v._starttime)

	for chunkIndex, chunk := range v.track.Chunks {
		end := sampleIndex + int(chunk.SamplesPerChunk)
		offset := int64(chunk.DataOffset)
		for ; sampleIndex < end && sampleIndex < len(v.track.Samples); sampleIndex++ {
			sample := v.track.Samples[sampleIndex]
			if sample.Size == 0 {
				continue
			}
			if stssIdx < len(v.stss) && v.stss[stssIdx] == uint32(sampleIndex+1) {
				idr.chunkIdx = chunkIndex
				idr.sampleIdx = sampleIndex
				idr.sampleEnd = end
				idr.decodingTime = decodingTime
				idr.dataOffset = offset
				stssIdx++
			}
			decodingTime += int64(sample.TimeDelta)
			if timestamp < decodingTime+int64(sample.CompositionTimeOffset) {
				if !force && idr.sampleIdx <= v.read.sampleIdx && v.read.sampleIdx <= sampleIndex+1 {
					return false, nil
				}
				v.read = idr
				return true, nil
			}
			offset += int64(sample.Size)
		}
	}
	return false, fmt.Errorf("out of range %d", timestamp)
}
